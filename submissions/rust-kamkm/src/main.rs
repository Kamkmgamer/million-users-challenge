use std::convert::Infallible;
use std::env;
use std::net::{IpAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use flate2::write::GzEncoder;
use flate2::Compression;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::Serialize;
use serde_json::Value;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{timeout, Duration};
use tokio_postgres::{error::SqlState, NoTls, Row};

const FEED_SQL: &str = "SELECT p.id, p.body, p.created_at, u.username, (SELECT count(*) FROM likes l WHERE l.post_id = p.id)::int AS like_count FROM posts p JOIN users u ON u.id = p.user_id ORDER BY p.created_at DESC, p.id DESC LIMIT 20";
const POST_BY_ID_SQL: &str = "SELECT p.id, p.body, p.created_at, u.username, (SELECT count(*) FROM likes l WHERE l.post_id = p.id)::int AS like_count FROM posts p JOIN users u ON u.id = p.user_id WHERE p.id = $1";
const MAX_REQUEST_BODY: usize = 16 * 1024;

type HttpBody = Full<Bytes>;

#[derive(Clone)]
struct AppState {
    pool: Pool,
    jwt_secret: Arc<[u8]>,
    started: Instant,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
}

#[derive(Serialize)]
struct PostBody {
    id: i64,
    body: String,
    created_at: String,
    author: String,
    like_count: i32,
}

#[derive(Serialize)]
struct FeedBody {
    posts: Vec<PostBody>,
}

#[derive(Serialize)]
struct PostResponse {
    post: PostBody,
}

#[derive(Serialize)]
struct LikeResponse {
    liked: bool,
    already_liked: bool,
    post_id: i64,
}

#[derive(Serialize)]
struct HealthBody {
    status: &'static str,
    db: &'static str,
    uptime_s: u64,
}

#[derive(Serialize)]
struct DegradedBody<'a> {
    status: &'static str,
    db: &'static str,
    error: &'a str,
}

#[derive(Clone)]
struct AuthUser {
    id: i64,
    username: String,
}

#[derive(Debug)]
enum AuthError {
    Missing,
    Invalid,
    Payload,
}

impl AppState {
    async fn handle(&self, req: Request<Incoming>) -> Response<HttpBody> {
        let method = req.method().clone();
        let path = req.uri().path();
        let wants_gzip = accepts_gzip(req.headers());

        if method == Method::GET && path == "/health" {
            return self.health(wants_gzip).await;
        }
        if method == Method::GET && path == "/feed" {
            return self.feed(wants_gzip).await;
        }
        if method == Method::GET {
            if let Some(id_text) = path.strip_prefix("/posts/") {
                if !id_text.is_empty() && !id_text.contains('/') {
                    let id = match parse_post_id(id_text) {
                        Some(id) => id,
                        None => {
                            return error_response(
                                StatusCode::BAD_REQUEST,
                                "invalid post id",
                                wants_gzip,
                            )
                        }
                    };
                    return self.get_post(id, wants_gzip).await;
                }
            }
            return error_response(StatusCode::NOT_FOUND, "not found", wants_gzip);
        }
        if method == Method::POST && path == "/posts" {
            let user = match authenticate(req.headers(), &self.jwt_secret) {
                Ok(user) => user,
                Err(err) => return auth_error_response(err, wants_gzip),
            };
            let body = match read_body(req).await {
                Ok(body) => body,
                Err(BodyError::TooLarge) => {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        "malformed JSON body",
                        wants_gzip,
                    )
                }
                Err(BodyError::Read) => {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal server error",
                        wants_gzip,
                    )
                }
            };
            return self.create_post(user, &body, wants_gzip).await;
        }
        if method == Method::POST {
            if let Some(id_text) = path.strip_prefix("/posts/") {
                if let Some(id_text) = id_text.strip_suffix("/like") {
                    // Authentication deliberately precedes id validation, as required by the API contract.
                    let user = match authenticate(req.headers(), &self.jwt_secret) {
                        Ok(user) => user,
                        Err(err) => return auth_error_response(err, wants_gzip),
                    };
                    if id_text.is_empty() || id_text.contains('/') {
                        return error_response(StatusCode::NOT_FOUND, "not found", wants_gzip);
                    }
                    let id = match parse_post_id(id_text) {
                        Some(id) => id,
                        None => {
                            return error_response(
                                StatusCode::BAD_REQUEST,
                                "invalid post id",
                                wants_gzip,
                            )
                        }
                    };
                    return self.like_post(user.id, id, wants_gzip).await;
                }
            }
            return error_response(StatusCode::NOT_FOUND, "not found", wants_gzip);
        }
        error_response(StatusCode::NOT_FOUND, "not found", wants_gzip)
    }

    async fn health(&self, gzip: bool) -> Response<HttpBody> {
        match timeout(Duration::from_secs(5), self.pool.get()).await {
            Ok(Ok(client)) => match client.query_one("SELECT 1", &[]).await {
                Ok(_) => json_response(
                    StatusCode::OK,
                    &HealthBody {
                        status: "ok",
                        db: "ok",
                        uptime_s: self.started.elapsed().as_secs(),
                    },
                    gzip,
                ),
                Err(err) => degraded_response(&err.to_string(), gzip),
            },
            Ok(Err(err)) => degraded_response(&err.to_string(), gzip),
            Err(_) => degraded_response("database pool timeout", gzip),
        }
    }

    async fn feed(&self, gzip: bool) -> Response<HttpBody> {
        let client = match pool_client(&self.pool).await {
            Ok(client) => client,
            Err(_) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error",
                    gzip,
                )
            }
        };
        let rows = match client.query(FEED_SQL, &[]).await {
            Ok(rows) => rows,
            Err(_) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error",
                    gzip,
                )
            }
        };
        let mut posts = Vec::with_capacity(20);
        for row in rows {
            match post_from_row(&row) {
                Some(post) => posts.push(post),
                None => {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal server error",
                        gzip,
                    )
                }
            }
        }
        json_response(StatusCode::OK, &FeedBody { posts }, gzip)
    }

    async fn get_post(&self, id: i64, gzip: bool) -> Response<HttpBody> {
        let client = match pool_client(&self.pool).await {
            Ok(client) => client,
            Err(_) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error",
                    gzip,
                )
            }
        };
        let row = match client.query_opt(POST_BY_ID_SQL, &[&id]).await {
            Ok(row) => row,
            Err(_) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error",
                    gzip,
                )
            }
        };
        let Some(row) = row else {
            return error_response(StatusCode::NOT_FOUND, "post not found", gzip);
        };
        let Some(post) = post_from_row(&row) else {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error",
                gzip,
            );
        };
        json_response(StatusCode::OK, &PostResponse { post }, gzip)
    }

    async fn create_post(&self, user: AuthUser, body: &[u8], gzip: bool) -> Response<HttpBody> {
        let body = match parse_post_body(body) {
            Ok(body) => body,
            Err(message) => return error_response(StatusCode::BAD_REQUEST, message, gzip),
        };
        let client = match pool_client(&self.pool).await {
            Ok(client) => client,
            Err(_) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error",
                    gzip,
                )
            }
        };
        let row = match client
            .query_one(
                "INSERT INTO posts (user_id, body) VALUES ($1, $2) RETURNING id, created_at",
                &[&user.id, &body],
            )
            .await
        {
            Ok(row) => row,
            Err(_) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error",
                    gzip,
                )
            }
        };
        let id: i64 = row.get(0);
        let created_at: DateTime<Utc> = row.get(1);
        let post = PostBody {
            id,
            body,
            created_at: format_timestamp(created_at),
            author: user.username,
            like_count: 0,
        };
        json_response(StatusCode::CREATED, &PostResponse { post }, gzip)
    }

    async fn like_post(&self, user_id: i64, post_id: i64, gzip: bool) -> Response<HttpBody> {
        let client = match pool_client(&self.pool).await {
            Ok(client) => client,
            Err(_) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error",
                    gzip,
                )
            }
        };
        let inserted = match client
            .execute(
                "INSERT INTO likes (user_id, post_id) VALUES ($1, $2) ON CONFLICT (user_id, post_id) DO NOTHING",
                &[&user_id, &post_id],
            )
            .await
        {
            Ok(count) => count,
            Err(err) => {
                if err
                    .as_db_error()
                    .is_some_and(|db_err| db_err.code() == &SqlState::FOREIGN_KEY_VIOLATION)
                {
                    return error_response(StatusCode::NOT_FOUND, "post not found", gzip);
                }
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error", gzip);
            }
        };
        let already_liked = inserted == 0;
        let status = if already_liked {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        };
        json_response(
            status,
            &LikeResponse {
                liked: true,
                already_liked,
                post_id,
            },
            gzip,
        )
    }
}

async fn pool_client(pool: &Pool) -> Result<deadpool_postgres::Client, ()> {
    timeout(Duration::from_secs(5), pool.get())
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
}

fn post_from_row(row: &Row) -> Option<PostBody> {
    Some(PostBody {
        id: row.try_get(0).ok()?,
        body: row.try_get(1).ok()?,
        created_at: format_timestamp(row.try_get(2).ok()?),
        author: row.try_get(3).ok()?,
        like_count: row.try_get(4).ok()?,
    })
}

fn format_timestamp(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn parse_post_id(value: &str) -> Option<i64> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let id = value.parse::<i64>().ok()?;
    (id > 0).then_some(id)
}

fn parse_post_body(body: &[u8]) -> Result<String, &'static str> {
    let value: Value = serde_json::from_slice(body).map_err(|_| "malformed JSON body")?;
    let text = value
        .as_object()
        .and_then(|obj| obj.get("body"))
        .and_then(Value::as_str)
        .map(str::trim)
        .ok_or("body is required")?;
    if text.is_empty() {
        return Err("body is required");
    }
    if text.chars().count() > 500 {
        return Err("body must be at most 500 characters");
    }
    Ok(text.to_owned())
}

enum BodyError {
    TooLarge,
    Read,
}

async fn read_body(req: Request<Incoming>) -> Result<Vec<u8>, BodyError> {
    if req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > MAX_REQUEST_BODY)
    {
        return Err(BodyError::TooLarge);
    }
    let mut incoming = req.into_body();
    let mut bytes = Vec::with_capacity(256);
    while let Some(frame) = incoming.frame().await {
        let frame = frame.map_err(|_| BodyError::Read)?;
        if let Ok(data) = frame.into_data() {
            if bytes.len().saturating_add(data.len()) > MAX_REQUEST_BODY {
                return Err(BodyError::TooLarge);
            }
            bytes.extend_from_slice(&data);
        }
    }
    Ok(bytes)
}

fn authenticate(headers: &hyper::HeaderMap, secret: &[u8]) -> Result<AuthUser, AuthError> {
    let Some(header) = headers.get(header::AUTHORIZATION) else {
        return Err(AuthError::Missing);
    };
    let Ok(value) = header.to_str() else {
        return Err(AuthError::Missing);
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return Err(AuthError::Missing);
    };
    if token.is_empty() {
        return Err(AuthError::Invalid);
    }

    let mut validation = Validation::new(Algorithm::HS256);
    validation.leeway = 0;
    validation.validate_exp = true;
    validation.validate_aud = false;
    validation.required_spec_claims.insert("exp".to_owned());
    let key = DecodingKey::from_secret(secret);
    let data = decode::<Value>(token, &key, &validation).map_err(|_| AuthError::Invalid)?;
    let claims = data.claims.as_object().ok_or(AuthError::Payload)?;
    let sub = claims
        .get("sub")
        .and_then(Value::as_str)
        .ok_or(AuthError::Payload)?;
    let id = parse_post_id(sub).ok_or(AuthError::Payload)?;
    let username = claims
        .get("username")
        .and_then(Value::as_str)
        .ok_or(AuthError::Payload)?;
    Ok(AuthUser {
        id,
        username: username.to_owned(),
    })
}

fn auth_error_response(error: AuthError, gzip: bool) -> Response<HttpBody> {
    match error {
        AuthError::Missing => {
            error_response(StatusCode::UNAUTHORIZED, "missing bearer token", gzip)
        }
        AuthError::Invalid => {
            error_response(StatusCode::UNAUTHORIZED, "invalid or expired token", gzip)
        }
        AuthError::Payload => {
            error_response(StatusCode::UNAUTHORIZED, "invalid token payload", gzip)
        }
    }
}

fn error_response(status: StatusCode, message: &'static str, gzip: bool) -> Response<HttpBody> {
    json_response(status, &ErrorBody { error: message }, gzip)
}

fn degraded_response(error: &str, gzip: bool) -> Response<HttpBody> {
    json_response(
        StatusCode::SERVICE_UNAVAILABLE,
        &DegradedBody {
            status: "degraded",
            db: "unreachable",
            error,
        },
        gzip,
    )
}

fn json_response<T: Serialize>(
    status: StatusCode,
    value: &T,
    force_gzip: bool,
) -> Response<HttpBody> {
    let plain = match serde_json::to_vec(value) {
        Ok(bytes) => bytes,
        Err(_) => br#"{"error":"internal server error"}"#.to_vec(),
    };
    let (body, compressed) = if force_gzip {
        let mut encoder = GzEncoder::new(Vec::with_capacity(plain.len()), Compression::fast());
        if std::io::Write::write_all(&mut encoder, &plain).is_ok() {
            match encoder.finish() {
                Ok(bytes) => (bytes, true),
                Err(_) => (plain, false),
            }
        } else {
            (plain, false)
        }
    } else {
        (plain, false)
    };
    let body_len = body.len();
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    let length = HeaderValue::from_str(&body_len.to_string()).expect("numeric content length");
    headers.insert(header::CONTENT_LENGTH, length);
    if compressed {
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        headers.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    }
    response
}

fn accepts_gzip(headers: &hyper::HeaderMap) -> bool {
    let Some(value) = headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    value.split(',').any(|part| {
        let mut segments = part.split(';');
        let coding = segments.next().map(str::trim).unwrap_or_default();
        if !coding.eq_ignore_ascii_case("gzip") {
            return false;
        }
        !segments.any(|parameter| {
            let mut kv = parameter.trim().splitn(2, '=');
            kv.next()
                .map(str::trim)
                .unwrap_or_default()
                .eq_ignore_ascii_case("q")
                && kv
                    .next()
                    .map(str::trim)
                    .is_some_and(|v| v == "0" || v == "0.0" || v == "0.00" || v == "0.000")
        })
    })
}

fn bind_listener(host: &str, port: u16) -> std::io::Result<TcpListener> {
    let host = if host == "*" || host.is_empty() {
        "0.0.0.0"
    } else {
        host
    };
    let addr = (host, port).to_socket_addrs()?.next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "host has no address")
    })?;
    let domain = match addr.ip() {
        IpAddr::V4(_) => Domain::IPV4,
        IpAddr::V6(_) => Domain::IPV6,
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(65_535)?;
    let listener = std::net::TcpListener::from(socket);
    TcpListener::from_std(listener)
}

async fn serve_listener(listener: TcpListener, state: Arc<AppState>) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let _ = stream.set_nodelay(true);
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(err) = serve_connection(stream, state).await {
                // Connection errors are normal when clients close idle keep-alive sockets.
                if !err.is_incomplete_message() {
                    let _ = err;
                }
            }
        });
    }
}

async fn serve_connection(stream: TcpStream, state: Arc<AppState>) -> Result<(), hyper::Error> {
    let io = TokioIo::new(stream);
    let service = service_fn(move |req: Request<Incoming>| {
        let state = Arc::clone(&state);
        async move { Ok::<_, Infallible>(state.handle(req).await) }
    });
    http1::Builder::new()
        .keep_alive(true)
        .max_buf_size(MAX_REQUEST_BODY)
        .serve_connection(io, service)
        .await
}

fn env_required(name: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    env::var(name).map_err(|_| format!("missing required environment variable {name}").into())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database_url = env_required("DATABASE_URL")?;
    let jwt_secret = env_required("JWT_SECRET")?;
    let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_owned());
    let port = env::var("PORT")
        .unwrap_or_else(|_| "3000".to_owned())
        .parse::<u16>()?;
    let extra_port = env::var("EXTRA_PORT")
        .ok()
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<u16>())
        .transpose()?;

    let pg_config: tokio_postgres::Config = database_url.parse()?;
    let mut pg_config = pg_config;
    pg_config.options("-c jit=off -c plan_cache_mode=force_generic_plan");
    let manager = Manager::from_config(
        pg_config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    // Keep PostgreSQL backend count near useful CPU parallelism. A large
    // pool only adds contention on the shared database; DB_POOL overrides it.
    let default_pool = cores.min(8).max(1);
    let pool_size = match env::var("DB_POOL") {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|&n| (1..=512).contains(&n))
            .expect("DB_POOL must be an integer from 1 through 512"),
        Err(_) => default_pool,
    };
    let pool = Pool::builder(manager)
        .max_size(pool_size)
        .runtime(Runtime::Tokio1)
        .build()?;

    let state = Arc::new(AppState {
        pool,
        jwt_secret: Arc::from(jwt_secret.into_bytes()),
        started: Instant::now(),
    });
    let listener = bind_listener(&host, port)?;
    let extra_listener = extra_port
        .map(|extra| bind_listener(&host, extra))
        .transpose()?;
    eprintln!(
        "rust-luna listening on {host}:{port}{} pool={pool_size} workers={cores}",
        extra_port.map_or_else(String::new, |p| format!(" and {host}:{p}"))
    );

    let primary_task = tokio::spawn(serve_listener(listener, Arc::clone(&state)));
    let extra_task =
        extra_listener.map(|listener| tokio::spawn(serve_listener(listener, Arc::clone(&state))));
    tokio::select! {
        result = primary_task => { result??; }
        result = async {
            if let Some(task) = extra_task {
                task.await??;
            } else {
                std::future::pending::<()>().await;
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        } => { result?; }
        _ = tokio::signal::ctrl_c() => {}
    }
    Ok(())
}
