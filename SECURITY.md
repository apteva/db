# Security

This is an alpha database. Report suspected vulnerabilities privately through
GitHub's private vulnerability reporting for apteva/db when available; do not
include credentials or sensitive database contents in a public issue.

Use TLS and a strong protected server token. The alpha token grants full database
access; there is no per-user authorization. Only feed trusted local files to
Import/Restore, and keep backups protected like the original database. The HTTP
server rejects filesystem-path operations from remote clients.
