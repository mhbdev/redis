# Security policy

Please report security vulnerabilities privately through the repository security
advisories page. Do not include Redis URLs, passwords, tokens, production keys, or
customer data in a public issue.

The package does not read environment variables, log credentials, or place raw
rate-limit identifiers directly in Redis keys. Consumers remain responsible for
protecting Redis credentials and configuring network access, TLS, ACLs, and least
privilege permissions.
