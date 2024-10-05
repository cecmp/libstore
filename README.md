# libstore

`libstore` is a Go package offering a flexible key-value storage interface with support for multiple backends.
The driving idea behind the design is to ensure the ability to migrate data from one backend to another and to
chain backends.

## Features
- **In-Memory (`InMemoryOps`)**: Fast, ephemeral storage for testing or caching.
- **PostgreSQL (`dbOps`)**: Persistent, versioned storage.
- **AWS S3 (`S3Ops`)**: Scalable cloud-based storage.
- **Encryption (`CryptStore`)**: Encrypts data before storage and decrypts on retrieval, ideal for client-side encryption with S3.
