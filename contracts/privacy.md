# Privacy and public-data rules

StreamPulse public fixtures and generated data are synthetic. They must not
contain:

- complete client IP addresses;
- cookies, authorization headers, or signed URL query strings;
- raw User-Agent strings unless explicitly synthetic;
- user accounts, device identifiers, phone numbers, or email addresses;
- private video URLs or real production hosts.

Permitted URL-related fields are synthetic or hashed `content_id`, enumerated
`object_type`, and a query-free templated `path_template`. `network_id` must be
synthetic, anonymous, or `UNKNOWN`. Session identifiers use one-way SHA-256
values prefixed by `sha256:`.

MVP synthetic normal traffic is unsampled so correctness can be checked. A
future sampling stage must retain errors, timeouts, fallbacks, and rebuffer
events, use stable deterministic hashing, and attach `sample_weight` where
count/byte estimates are reconstructed. Percentiles are never reconstructed by
simply multiplying by sample weight.
