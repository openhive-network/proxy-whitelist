-- plpython3u functions for extracting and resolving image URLs from post content.

CREATE OR REPLACE FUNCTION proxy_whitelist_app.extract_image_urls(
    body TEXT,
    json_metadata TEXT
)
RETURNS TEXT[]
LANGUAGE plpython3u
STABLE
AS $BODY$
    import re, json

    urls = set()
    IMAGE_RE = re.compile(
        r'https?://[^\s\)\]"<>]+\.(?:tiff?|jpe?g|gif|png|svg|ico|webp|heic|heif|jfif|bmp|avif|apng)(?:\?[^\s\)\]"<>]*)?',
        re.IGNORECASE
    )
    HTTP_RE = re.compile(r'^https?://', re.IGNORECASE)

    # Extract from body (regex filter — only known image extensions)
    if body:
        for m in IMAGE_RE.finditer(body):
            urls.add(m.group(0))

    # Extract from json_metadata 'image' array
    # Trust the posting app: any HTTP URL in the image array is an image
    if json_metadata:
        try:
            meta = json.loads(json_metadata)
            if not isinstance(meta, dict):
                return list(urls)
            for img in meta.get('image', []):
                if isinstance(img, str) and HTTP_RE.match(img):
                    urls.add(img)
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

    return list(urls)
$BODY$;

GRANT EXECUTE ON FUNCTION proxy_whitelist_app.extract_image_urls(TEXT, TEXT)
    TO haf_admin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.extract_image_urls(TEXT, TEXT)
    TO proxy_whitelist_user;


CREATE OR REPLACE FUNCTION proxy_whitelist_app.resolve_proxy_url(url TEXT)
RETURNS TEXT[]
LANGUAGE plpython3u
STABLE
AS $BODY$
    import re

    # Skip uploads (DQm... hashes) — these are served via upload path, not proxy
    if re.match(r'https?://images\.hive\.blog/DQm', url):
        return None

    result = set()
    result.add(url)  # always include raw URL

    # Proxy-wrapped: images.hive.blog/p/{base58} or steemitimages.com/p/{base58}
    m = re.match(r'https?://(?:images\.hive\.blog|steemitimages\.com)/p/([A-Za-z1-9]+)', url)
    if m:
        try:
            import base58
            decoded = base58.b58decode(m.group(1))
            if len(decoded) > 2:
                raw = decoded.decode('utf-8', errors='replace')
                if raw.startswith('http'):
                    # Plain base58 encoding (no multihash prefix)
                    result.add(raw)
                else:
                    # Multihash-prefixed encoding: strip 2-byte prefix
                    inner = decoded[2:].decode('utf-8', errors='replace')
                    if inner.startswith('http'):
                        result.add(inner)
        except Exception:
            pass
        return list(result)

    # Legacy format: images.hive.blog/{W}x{H}/{url}
    m = re.match(r'https?://(?:images\.hive\.blog|steemitimages\.com)/\d+x\d+/(https?://.+)', url)
    if m:
        nested = m.group(1)
        # steemit.com/ipfs/ -> ipfs.io/ipfs/ rewrite
        nested = re.sub(r'https?://steemit\.com/ipfs/', 'https://ipfs.io/ipfs/', nested)
        result.add(nested)
        return list(result)

    # steemitimages.com/0x0/{url}
    m = re.match(r'https?://steemitimages\.com/0x0/(https?://.+)', url)
    if m:
        result.add(m.group(1))
        return list(result)

    return list(result)
$BODY$;

GRANT EXECUTE ON FUNCTION proxy_whitelist_app.resolve_proxy_url(TEXT)
    TO haf_admin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.resolve_proxy_url(TEXT)
    TO proxy_whitelist_user;


-- Extract profile_image and cover_image from account metadata JSON fields.
CREATE OR REPLACE FUNCTION proxy_whitelist_app.extract_profile_urls(
    posting_json_metadata TEXT,
    json_metadata TEXT
)
RETURNS TEXT[]
LANGUAGE plpython3u
STABLE
AS $BODY$
    import json

    urls = set()

    for raw in [posting_json_metadata, json_metadata]:
        if not raw:
            continue
        try:
            meta = json.loads(raw)
            if not isinstance(meta, dict):
                continue
            profile = meta.get('profile', meta)
            if isinstance(profile, dict):
                for key in ('profile_image', 'cover_image'):
                    val = profile.get(key)
                    if isinstance(val, str) and val.startswith('http'):
                        urls.add(val)
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

    return list(urls) if urls else None
$BODY$;

GRANT EXECUTE ON FUNCTION proxy_whitelist_app.extract_profile_urls(TEXT, TEXT)
    TO haf_admin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION proxy_whitelist_app.extract_profile_urls(TEXT, TEXT)
    TO proxy_whitelist_user;
