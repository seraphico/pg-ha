---
name: Bug report
about: Report a bug or unexpected behavior in pg-ha
title: "[Bug]: "
labels: bug, help wanted
assignees: seraphico

---

**Describe the bug**
A clear and concise description of what the bug is.

**Expected behavior**
What you expected to happen instead.

**Steps to reproduce**
1. Start cluster with ...
2. Run ...
3. Observe ...

**Component**
Which part of pg-ha is affected? (Raft/DCS, HA loop, Proxy, PostgreSQL management, API, Bootstrap, Config)

**Environment:**
- pg-ha version: [e.g. v0.1.5]
- PostgreSQL version: [e.g. 16.3]
- Deployment: [Docker Compose / Binary / Kubernetes]
- Cluster size: [e.g. 3 nodes]
- OS: [e.g. Ubuntu 22.04]

**Logs**
```
Paste relevant log output here (JSON logs preferred, please redact secrets)
```

**Configuration**
```yaml
# Relevant parts of your pg-ha config (redact passwords)
```

**Additional context**
Add any other context, screenshots, or information about the problem here.
