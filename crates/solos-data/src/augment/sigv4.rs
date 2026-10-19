//! AWS Signature Version 4 for unauthenticated-looking S3 GETs against requester-pays buckets:
//! the canonical request, the string to sign, the derived key and the `Authorization` header.
//! Only what the Hyperliquid archive needs (GET, an empty payload, a handful of headers); the
//! secret never leaves this module and no header value is logged.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// SHA-256 of an empty payload, the `x-amz-content-sha256` of every GET.
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// One request to sign.
pub struct Request<'a> {
    /// `GET`.
    pub method: &'a str,
    /// The `Host` header as sent (with `:port` when not the default).
    pub host: &'a str,
    /// The path, not yet encoded (`/market_data/20251001/0/l2Book/BTC.lz4`).
    pub path: &'a str,
    /// Query pairs, not yet encoded.
    pub query: &'a [(String, String)],
    /// Headers to sign besides `host`, names lower-case.
    pub headers: &'a [(String, String)],
    /// `YYYYMMDDTHHMMSSZ`.
    pub amz_date: &'a str,
    /// Bucket region.
    pub region: &'a str,
}

/// RFC 3986 encoding as SigV4 wants it; `/` is kept in paths and encoded in query values.
#[must_use]
pub fn uri_encode(value: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// The canonical query string: pairs encoded, sorted by name then value, joined with `&`. The
/// request URL must carry exactly this string.
#[must_use]
pub fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (uri_encode(k, false), uri_encode(v, false)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn sorted_headers(request: &Request<'_>) -> Vec<(String, String)> {
    let mut headers: Vec<(String, String)> = request
        .headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    headers.push(("host".into(), request.host.to_owned()));
    headers.sort();
    headers
}

/// `host;x-amz-content-sha256;…`
#[must_use]
pub fn signed_header_names(request: &Request<'_>) -> String {
    sorted_headers(request)
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";")
}

/// The canonical request.
#[must_use]
pub fn canonical_request(request: &Request<'_>) -> String {
    let headers = sorted_headers(request);
    let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let payload = request
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-content-sha256"))
        .map_or(EMPTY_SHA256, |(_, v)| v.as_str());
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method,
        uri_encode(request.path, true),
        canonical_query(request.query),
        canonical_headers,
        signed_header_names(request),
        payload
    )
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes any key length");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn scope(request: &Request<'_>) -> String {
    format!(
        "{}/{}/s3/aws4_request",
        &request.amz_date[..8.min(request.amz_date.len())],
        request.region
    )
}

/// The hex signature of a request.
#[must_use]
pub fn signature(secret: &str, request: &Request<'_>) -> String {
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        request.amz_date,
        scope(request),
        hex::encode(Sha256::digest(canonical_request(request).as_bytes()))
    );
    let date = &request.amz_date[..8.min(request.amz_date.len())];
    let key = hmac(format!("AWS4{secret}").as_bytes(), date);
    let key = hmac(&key, request.region);
    let key = hmac(&key, "s3");
    let key = hmac(&key, "aws4_request");
    hex::encode(hmac(&key, &string_to_sign))
}

/// The `Authorization` header value.
#[must_use]
pub fn authorization(access_key_id: &str, secret: &str, request: &Request<'_>) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={access_key_id}/{}, SignedHeaders={}, Signature={}",
        scope(request),
        signed_header_names(request),
        signature(secret, request)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // The examples of the S3 SigV4 documentation ("Signature calculations for the
    // Authorization header", GET Object and GET Bucket).
    const AKID: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    fn headers(extra: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = extra
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        out.push(("x-amz-content-sha256".into(), EMPTY_SHA256.into()));
        out.push(("x-amz-date".into(), "20130524T000000Z".into()));
        out
    }

    #[test]
    fn get_object_example() {
        let headers = headers(&[("range", "bytes=0-9")]);
        let request = Request {
            method: "GET",
            host: "examplebucket.s3.amazonaws.com",
            path: "/test.txt",
            query: &[],
            headers: &headers,
            amz_date: "20130524T000000Z",
            region: "us-east-1",
        };
        assert_eq!(
            signed_header_names(&request),
            "host;range;x-amz-content-sha256;x-amz-date"
        );
        assert_eq!(
            signature(SECRET, &request),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
        assert!(
            authorization(AKID, SECRET, &request).starts_with(
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;"
            )
        );
    }

    #[test]
    fn list_bucket_example() {
        let headers = headers(&[]);
        let query = vec![
            ("prefix".to_owned(), "J".to_owned()),
            ("max-keys".to_owned(), "2".to_owned()),
        ];
        let request = Request {
            method: "GET",
            host: "examplebucket.s3.amazonaws.com",
            path: "/",
            query: &query,
            headers: &headers,
            amz_date: "20130524T000000Z",
            region: "us-east-1",
        };
        assert_eq!(canonical_query(&query), "max-keys=2&prefix=J");
        assert_eq!(
            signature(SECRET, &request),
            "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        );
    }

    #[test]
    fn encoding() {
        assert_eq!(uri_encode("a b/c:d", true), "a%20b/c%3Ad");
        assert_eq!(uri_encode("x/y", false), "x%2Fy");
    }
}
