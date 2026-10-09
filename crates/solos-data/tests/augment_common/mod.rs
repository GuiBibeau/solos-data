//! An in-process HTTP/1.1 server for the augmentation lane's tests: routes keyed by path,
//! handlers that see the method, query and body, a hit log, and zip helpers for Binance dumps.

#![allow(dead_code)]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

/// One parsed request.
#[derive(Clone, Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    /// One query parameter, URL-decoded for `%XX` escapes.
    pub fn param(&self, name: &str) -> Option<String> {
        self.query.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            (k == name).then(|| decode(v))
        })
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

fn decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let escaped = (bytes[i] == b'%' && i + 2 < bytes.len())
            .then(|| u8::from_str_radix(&text[i + 1..i + 3], 16).ok())
            .flatten();
        match escaped {
            Some(byte) => {
                out.push(byte);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// One response.
#[derive(Clone, Debug)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn ok(body: Vec<u8>) -> Response {
        Response {
            status: 200,
            headers: Vec::new(),
            body,
        }
    }

    pub fn json(value: &serde_json::Value) -> Response {
        Response::ok(value.to_string().into_bytes())
    }

    pub fn status(status: u16) -> Response {
        Response {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Response {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }
}

type Handler = Arc<dyn Fn(&Request) -> Response + Send + Sync>;

/// The server.
pub struct Server {
    pub base: String,
    routes: Arc<Mutex<HashMap<String, Handler>>>,
    hits: Arc<Mutex<Vec<Request>>>,
}

impl Server {
    pub fn start() -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let routes: Arc<Mutex<HashMap<String, Handler>>> = Arc::new(Mutex::new(HashMap::new()));
        let hits: Arc<Mutex<Vec<Request>>> = Arc::new(Mutex::new(Vec::new()));
        let (routes_thread, hits_thread) = (Arc::clone(&routes), Arc::clone(&hits));
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let (routes, hits) = (Arc::clone(&routes_thread), Arc::clone(&hits_thread));
                std::thread::spawn(move || serve(stream, &routes, &hits));
            }
        });
        Server {
            base: format!("http://127.0.0.1:{port}"),
            routes,
            hits,
        }
    }

    /// Route a path (without query) to a handler.
    pub fn route(
        &self,
        path: &str,
        handler: impl Fn(&Request) -> Response + Send + Sync + 'static,
    ) {
        self.routes
            .lock()
            .unwrap()
            .insert(path.to_owned(), Arc::new(handler));
    }

    /// Route a path to a fixed body.
    pub fn serve_bytes(&self, path: &str, body: Vec<u8>) {
        self.route(path, move |_| Response::ok(body.clone()));
    }

    /// Route a path to a fixed JSON value.
    pub fn serve_json(&self, path: &str, value: serde_json::Value) {
        self.route(path, move |_| Response::json(&value));
    }

    pub fn hits(&self) -> Vec<Request> {
        self.hits.lock().unwrap().clone()
    }

    pub fn hits_of(&self, path: &str) -> usize {
        self.hits
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.path == path)
            .count()
    }

    pub fn hit_count(&self) -> usize {
        self.hits.lock().unwrap().len()
    }
}

fn serve(
    mut stream: TcpStream,
    routes: &Mutex<HashMap<String, Handler>>,
    hits: &Mutex<Vec<Request>>,
) {
    let Some(request) = read_request(&mut stream) else {
        return;
    };
    let handler = routes.lock().unwrap().get(&request.path).cloned();
    hits.lock().unwrap().push(request.clone());
    let response = match handler {
        Some(handler) => handler(&request),
        None => Response::status(404),
    };
    let mut head = format!(
        "HTTP/1.1 {} X\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        response.body.len()
    );
    for (name, value) in &response.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(&response.body);
    let _ = stream.flush();
}

fn read_request(stream: &mut TcpStream) -> Option<Request> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(pos) = find(&buffer, b"\r\n\r\n") {
            break pos;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split(' ');
    let method = parts.next()?.to_owned();
    let target = parts.next()?;
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
        .collect();
    let length: usize = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let mut body = buffer[header_end + 4..].to_vec();
    while body.len() < length {
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    Some(Request {
        method,
        path: path.to_owned(),
        query: query.to_owned(),
        headers,
        body,
    })
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// A zip holding one deflated file.
pub fn zip_of(name: &str, content: &[u8]) -> Vec<u8> {
    let mut buffer = std::io::Cursor::new(Vec::new());
    {
        let mut writer = zip::ZipWriter::new(&mut buffer);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        writer.start_file(name, options).unwrap();
        writer.write_all(content).unwrap();
        writer.finish().unwrap();
    }
    buffer.into_inner()
}

/// `<sha256>  <name>` as Binance publishes it.
pub fn checksum_of(bytes: &[u8], name: &str) -> Vec<u8> {
    format!("{}  {name}\n", solos_data::fsutil::sha256_hex(bytes)).into_bytes()
}

/// An S3 `ListBucketResult` for the given keys.
pub fn listing_xml(
    prefix: &str,
    keys: &[String],
    truncated: bool,
    next_marker: Option<&str>,
) -> Vec<u8> {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>data.binance.vision</Name><Prefix>{prefix}</Prefix><Marker></Marker>"
    );
    if let Some(marker) = next_marker {
        xml.push_str(&format!("<NextMarker>{marker}</NextMarker>"));
    }
    xml.push_str(&format!(
        "<MaxKeys>500</MaxKeys><Delimiter>/</Delimiter><IsTruncated>{truncated}</IsTruncated>"
    ));
    for key in keys {
        xml.push_str(&format!("<Contents><Key>{key}</Key><LastModified>2026-10-08T06:49:30.000Z</LastModified><ETag>&quot;x&quot;</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>"));
    }
    xml.push_str("</ListBucketResult>");
    xml.into_bytes()
}

/// A fresh temporary directory.
pub fn tempdir(prefix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The repository fixture directory.
pub fn fixture(name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/data/augment")
        .join(name);
    std::fs::read(path).unwrap()
}
