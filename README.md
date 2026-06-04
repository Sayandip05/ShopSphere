# 🛒 ShopSphere — Production-Grade E-Commerce Microservices Platform

<div align="center">

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Django](https://img.shields.io/badge/Django-4.2-092E20?style=for-the-badge&logo=django&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?style=for-the-badge&logo=mongodb&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![Elasticsearch](https://img.shields.io/badge/Elasticsearch-005571?style=for-the-badge&logo=elasticsearch&logoColor=white)
![NGINX](https://img.shields.io/badge/NGINX-009639?style=for-the-badge&logo=nginx&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)

**A fully production-grade, Amazon-like multi-category e-commerce backend built with a microservices architecture.**  
Designed for scalability, fault tolerance, and real-world system design interview preparation.

[Architecture Diagrams (Miro)](https://miro.com/app/board/uXjVHMoCKt8=/) · [API Docs](#api-documentation) · [Setup Guide](#getting-started) · [Team](#team-ownership)

</div>

---

## 📋 Table of Contents

1. [Project Overview](#1-project-overview)
2. [System Architecture](#2-system-architecture)
3. [Tech Stack](#3-tech-stack)
4. [Microservices Overview](#4-microservices-overview)
5. [Database Design](#5-database-design)
6. [Service-by-Service Documentation](#6-service-by-service-documentation)
   - [Auth Service](#61-auth-service)
   - [User Service](#62-user-service)
   - [Product Service](#63-product-service)
   - [Cart & Wishlist Service](#64-cart--wishlist-service)
   - [Order Service](#65-order-service)
   - [Payment Service](#66-payment-service)
   - [Search Service](#67-search-service)
   - [AI Recommendation Service](#68-ai-recommendation-service)
   - [Notification Service](#69-notification-service)
   - [Analytics Service](#610-analytics-service)
   - [WebSocket Gateway](#611-websocket-gateway)
   - [GraphQL Gateway](#612-graphql-gateway)
7. [Kafka Event Architecture](#7-kafka-event-architecture)
8. [NGINX & Load Balancer](#8-nginx--load-balancer)
9. [WebSocket Architecture](#9-websocket-architecture)
10. [Elasticsearch Integration](#10-elasticsearch-integration)
11. [CI/CD Pipeline](#11-cicd-pipeline)
12. [Getting Started](#12-getting-started)
13. [Environment Variables](#13-environment-variables)
14. [Docker Compose Reference](#14-docker-compose-reference)
15. [API Documentation](#15-api-documentation)
16. [Folder Structure](#16-folder-structure)
17. [Development Workflow](#17-development-workflow)
18. [Testing Strategy](#18-testing-strategy)
19. [Implementation Roadmap](#19-implementation-roadmap)
20. [Team Ownership](#20-team-ownership)
21. [Best Practices](#21-best-practices)
22. [System Design Decisions](#22-system-design-decisions)
23. [Known Limitations & Future Work](#23-known-limitations--future-work)

---

## 1. Project Overview

ShopSphere is a **production-grade, backend-only e-commerce platform** modelled after Amazon — a general multi-category marketplace where buyers browse, search, and purchase products from multiple sellers. It is built from the ground up as a **microservices architecture** with a focus on:

- **Scalability** — every service scales independently via Docker + NGINX load balancing
- **Resilience** — Kafka-based async communication ensures no event is lost even if a consumer service crashes
- **Real-time** — WebSocket gateway pushes live order and payment status to connected clients
- **Intelligence** — AI-powered product recommendations using vector embeddings and Qdrant
- **Observability** — structured logging, health check endpoints, and correlation IDs across services
- **Interview-readiness** — covers every major system design topic (caching, message queues, search, pagination, state machines, reverse proxy, etc.)

### What This Project Is NOT
- This is a **backend-only** project. There is no frontend, no React, no HTML.
- It is not a toy CRUD app — every design decision is production-motivated.
- It does not cut corners on database choice — each service uses the database best suited for its access patterns.

### Who Built This
This project is a **2-person collaborative backend project** built by:
- **Sayandip** — Auth Service, Product Service, Cart & Wishlist Service, Search Service, AI Recommendation Service, NGINX/LB, CI/CD
- **Debabrata** — User Service, Order Service, Payment Service, Notification Service, Analytics Service, WebSocket Gateway, Kafka Setup

---

## 2. System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLIENT (Web / Mobile)                          │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │ HTTPS / WSS
┌──────────────────────────────────▼──────────────────────────────────────────┐
│                     NGINX Reverse Proxy (Port 80/443)                       │
│          SSL Termination · Rate Limiting · Static File Serving              │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────────┐
│                        Load Balancer (Round Robin)                          │
│                  Health Checks · Auto Pool Management                       │
└────┬──────────┬──────────┬───────┴───────┬──────────┬────────────┬──────────┘
     │          │          │               │          │            │
┌────▼──┐  ┌───▼───┐  ┌───▼───┐     ┌────▼──┐  ┌────▼──┐  ┌─────▼──┐
│Django │  │Django │  │Django │     │FastAPI│  │FastAPI│  │FastAPI │
│Inst.1 │  │Inst.2 │  │Inst.3 │     │Inst.1 │  │Inst.2 │  │Inst.3  │
└───────┘  └───────┘  └───────┘     └───────┘  └───────┘  └────────┘

CORE DJANGO SERVICES          AI & ASYNC FASTAPI SERVICES
├── Auth Service    :8001      ├── Search Service        :8007
├── User Service    :8002      ├── AI Recommendation     :8008
├── Product Service :8003      ├── Notification Service  :8009
├── Cart Service    :8004      └── Analytics Service     :8010
├── Order Service   :8005
└── Payment Service :8006

SPECIAL GATEWAYS
├── WebSocket Gateway (Django Channels) :8011
└── GraphQL Gateway (Strawberry)        :8012

EVENT BUS                      DATA LAYER
└── Apache Kafka + Zookeeper   ├── PostgreSQL  (Auth, Order)
    Topics:                    ├── MySQL       (User, Payment)
    ├── order.created          ├── MongoDB     (Product)
    ├── order.cancelled        ├── Redis       (Cart, Cache, WS Channel Layer)
    ├── payment.success        ├── Elasticsearch (Search)
    ├── payment.failed         ├── Qdrant VectorDB (AI Recommendations)
    ├── user.registered        └── ClickHouse  (Analytics)
    └── inventory.updated
```

> 📐 **Full Architecture Diagrams:** [Miro Board](https://miro.com/app/board/uXjVHMoCKt8=/)  
> Includes: HLD, Microservice Architecture, Kafka Flow, CI/CD, WebSocket, NGINX/LB, Elasticsearch, Service Communication, and all 6 LLDs.

---

## 3. Tech Stack

### Languages & Frameworks
| Technology | Version | Used For |
|---|---|---|
| Python | 3.11+ | All services |
| Django | 4.2 | Core CRUD services (Auth, User, Product, Cart, Order, Payment) |
| Django REST Framework | 3.14+ | REST API layer for all Django services |
| FastAPI | 0.104+ | High-throughput async services (Search, AI, Notification, Analytics) |
| Django Channels | 4.x | WebSocket gateway (real-time order updates) |
| Strawberry GraphQL | 0.x | GraphQL gateway wrapping User + Product services |

### Databases
| Database | Version | Used By | Reason |
|---|---|---|---|
| PostgreSQL | 15 | Auth Service, Order Service | ACID transactions, relational integrity |
| MySQL | 8.0 | User Service, Payment Service | Structured relational data, financial records |
| MongoDB | 7.0 | Product Service, Notification Service | Flexible schema for product attributes |
| Redis | 7.2 | Cart Service, Cache, WS Channel Layer | Sub-millisecond reads, TTL, Pub/Sub |
| Elasticsearch | 8.x | Search Service | Full-text search, facets, autocomplete |
| Qdrant | 1.x | AI Recommendation Service | Vector similarity search |
| ClickHouse | 23.x | Analytics Service | Columnar store for fast aggregations |

### Infrastructure & DevOps
| Technology | Used For |
|---|---|
| Docker + Docker Compose | Local development, containerisation |
| NGINX | Reverse proxy, SSL termination, rate limiting, load balancing |
| Apache Kafka + Zookeeper | Async inter-service event bus |
| GitHub Actions | CI/CD pipeline (lint, test, build, deploy) |

### Key Python Libraries
| Library | Purpose |
|---|---|
| `djangorestframework` | REST API framework |
| `djangorestframework-simplejwt` | JWT authentication |
| `drf-spectacular` | OpenAPI/Swagger auto-generation |
| `django-filter` | Filtering on list endpoints |
| `celery` | Async task queue (emails, background jobs) |
| `kafka-python` | Kafka producer/consumer |
| `elasticsearch-py` | Elasticsearch client |
| `qdrant-client` | Qdrant vector DB client |
| `sentence-transformers` | Product embedding generation |
| `mongoengine` | MongoDB ODM for Django |
| `pytest` + `pytest-django` | Testing |
| `flake8` + `black` | Linting and formatting |
| `python-dotenv` | Environment variable management |
| `argon2-cffi` | Password hashing |

---

## 4. Microservices Overview

| # | Service | Framework | Port | Database | Owner |
|---|---|---|---|---|---|
| 1 | Auth Service | Django + DRF | 8001 | PostgreSQL + Redis | Sayandip |
| 2 | User Service | Django + DRF | 8002 | MySQL | Debabrata |
| 3 | Product Service | Django + DRF | 8003 | MongoDB + Redis | Sayandip |
| 4 | Cart & Wishlist | Django + DRF | 8004 | Redis | Sayandip |
| 5 | Order Service | Django + DRF | 8005 | PostgreSQL | Debabrata |
| 6 | Payment Service | Django + DRF | 8006 | MySQL | Debabrata |
| 7 | Search Service | FastAPI | 8007 | Elasticsearch | Sayandip |
| 8 | AI Recommendation | FastAPI | 8008 | Qdrant | Sayandip |
| 9 | Notification Service | FastAPI | 8009 | MongoDB | Debabrata |
| 10 | Analytics Service | FastAPI | 8010 | ClickHouse | Debabrata |
| 11 | WebSocket Gateway | Django Channels | 8011 | Redis (channel layer) | Debabrata |
| 12 | GraphQL Gateway | Strawberry | 8012 | — (proxies User + Product) | Shared |

---

## 5. Database Design

### Why Polyglot Persistence?

Each service owns its own database, and no service ever queries another service's database directly. Services communicate only via REST APIs or Kafka events. This is the **database-per-service pattern** — the cornerstone of true microservice independence.

```
Service               Database        Why This DB
──────────────────────────────────────────────────────────────────
Auth Service        → PostgreSQL    ACID for credentials, FK constraints
User Service        → MySQL         Structured profiles, relational
Product Service     → MongoDB       Variable attribute schema per product
Cart & Wishlist     → Redis         Ephemeral, TTL-based, sub-ms reads
Order Service       → PostgreSQL    Complex state machine, ACID transactions
Payment Service     → MySQL         Financial records, audit trail, relational
Search Service      → Elasticsearch Full-text, facets, scoring, autocomplete
AI Recommendation   → Qdrant        Vector similarity search (embeddings)
Notification Svc    → MongoDB       Flexible notification templates/logs
Analytics Service   → ClickHouse    Columnar, blazing fast aggregations
WebSocket Gateway   → Redis         Pub/Sub channel layer for WS
```

---

## 6. Service-by-Service Documentation

---

### 6.1 Auth Service

**Port:** `8001`  
**Framework:** Django 4.2 + Django REST Framework  
**Database:** PostgreSQL (primary) + Redis (token blacklist)  
**Owner:** Sayandip

#### Responsibility
Handles all authentication and authorisation. Issues JWT access tokens and refresh tokens. Every other service trusts tokens issued by this service.

#### Database Schema

**Table: `auth_user`**
```sql
id            UUID          PRIMARY KEY DEFAULT gen_random_uuid()
email         VARCHAR(255)  UNIQUE NOT NULL  -- indexed
password_hash VARCHAR(128)  NOT NULL         -- argon2 hashed
role          ENUM          BUYER|SELLER|ADMIN DEFAULT 'BUYER'
is_active     BOOLEAN       DEFAULT TRUE
is_verified   BOOLEAN       DEFAULT FALSE
last_login    TIMESTAMPTZ   NULLABLE
created_at    TIMESTAMPTZ   DEFAULT NOW()
updated_at    TIMESTAMPTZ   DEFAULT NOW()
```

**Table: `refresh_token`**
```sql
id            UUID          PRIMARY KEY
user_id       UUID          FK → auth_user.id ON DELETE CASCADE  -- indexed
token_hash    VARCHAR(256)  NOT NULL  -- indexed
device_info   VARCHAR(255)  NULLABLE
ip_address    INET          NULLABLE
expires_at    TIMESTAMPTZ   NOT NULL
is_revoked    BOOLEAN       DEFAULT FALSE
created_at    TIMESTAMPTZ   DEFAULT NOW()
```

**Table: `email_verification`**
```sql
id            UUID          PRIMARY KEY
user_id       UUID          FK → auth_user.id
token         VARCHAR(64)   UNIQUE NOT NULL
expires_at    TIMESTAMPTZ   NOT NULL  -- NOW() + 24 hours
is_used       BOOLEAN       DEFAULT FALSE
```

**Table: `password_reset`**
```sql
id            UUID          PRIMARY KEY
user_id       UUID          FK → auth_user.id
token         VARCHAR(64)   UNIQUE NOT NULL
expires_at    TIMESTAMPTZ   NOT NULL  -- NOW() + 1 hour
is_used       BOOLEAN       DEFAULT FALSE
```

#### API Endpoints

| Method | Endpoint | Auth Required | Description |
|---|---|---|---|
| POST | `/api/v1/auth/register/` | No | Register new user |
| POST | `/api/v1/auth/login/` | No | Login, returns JWT pair |
| POST | `/api/v1/auth/refresh/` | No (refresh token) | Get new access token |
| POST | `/api/v1/auth/logout/` | Yes | Revoke refresh token |
| POST | `/api/v1/auth/verify-email/` | No | Verify email with token |
| POST | `/api/v1/auth/forgot-password/` | No | Request password reset |
| POST | `/api/v1/auth/reset-password/` | No | Reset password with token |
| GET  | `/api/v1/auth/me/` | Yes | Get current user basic info |
| GET  | `/health/` | No | Health check |

#### Token Strategy
- **Access Token:** JWT, expires in **15 minutes**, signed with RS256
- **Refresh Token:** Opaque token, hashed before storing, expires in **7 days**
- **Blacklist:** On logout, refresh token hash is stored in Redis with TTL matching expiry
- **Rotation:** Every refresh call issues a new refresh token and invalidates the old one

#### Kafka Events Published
- `user.registered` — on successful registration → consumed by Notification Service

#### Key Design Decisions
- Password hashed with **argon2** (more secure than bcrypt)
- Refresh tokens stored as **SHA-256 hashes** in DB — raw token never persisted
- `is_verified` flag blocks login until email verification is complete (configurable)
- `auth_user_id` (UUID) is the cross-service user identity — every other service stores this, not an internal user PK

---

### 6.2 User Service

**Port:** `8002`  
**Framework:** Django 4.2 + Django REST Framework  
**Database:** MySQL 8.0  
**Owner:** Debabrata

#### Responsibility
Manages user profiles, personal information, and delivery addresses. Does NOT store credentials — those live in Auth Service. Looks up users via `auth_user_id` from the JWT payload.

#### Database Schema

**Table: `user_profiles`**
```sql
id             CHAR(36)     PRIMARY KEY
auth_user_id   CHAR(36)     UNIQUE NOT NULL  -- from Auth Service JWT, indexed
first_name     VARCHAR(100) NOT NULL
last_name      VARCHAR(100) NOT NULL
phone          VARCHAR(15)  UNIQUE NULLABLE  -- indexed
email          VARCHAR(255) NOT NULL         -- denormalized from Auth
avatar_url     VARCHAR(500) NULLABLE
gender         ENUM         MALE|FEMALE|OTHER|PREFER_NOT
date_of_birth  DATE         NULLABLE
is_seller      BOOLEAN      DEFAULT FALSE
is_active      BOOLEAN      DEFAULT TRUE
created_at     DATETIME     DEFAULT CURRENT_TIMESTAMP
updated_at     DATETIME     ON UPDATE CURRENT_TIMESTAMP
```

**Table: `addresses`**
```sql
id             CHAR(36)     PRIMARY KEY
user_id        CHAR(36)     FK → user_profiles.id CASCADE  -- indexed
label          ENUM         HOME|WORK|OTHER
full_name      VARCHAR(200) NOT NULL
phone          VARCHAR(15)  NOT NULL
street_line1   VARCHAR(300) NOT NULL
street_line2   VARCHAR(300) NULLABLE
city           VARCHAR(100) NOT NULL  -- indexed
state          VARCHAR(100) NOT NULL
pincode        VARCHAR(10)  NOT NULL  -- indexed
country        VARCHAR(50)  DEFAULT 'IN'
latitude       DECIMAL(9,6) NULLABLE
longitude      DECIMAL(9,6) NULLABLE
is_default     BOOLEAN      DEFAULT FALSE
is_active      BOOLEAN      DEFAULT TRUE
created_at     DATETIME     DEFAULT CURRENT_TIMESTAMP
```

#### API Endpoints

| Method | Endpoint | Auth Required | Description |
|---|---|---|---|
| GET | `/api/v1/users/me/` | Yes | Get own profile |
| PATCH | `/api/v1/users/me/` | Yes | Update own profile |
| GET | `/api/v1/users/me/addresses/` | Yes | List all addresses |
| POST | `/api/v1/users/me/addresses/` | Yes | Add new address |
| GET | `/api/v1/users/me/addresses/{id}/` | Yes | Get single address |
| PATCH | `/api/v1/users/me/addresses/{id}/` | Yes | Edit address |
| DELETE | `/api/v1/users/me/addresses/{id}/` | Yes | Soft-delete address |
| PUT | `/api/v1/users/me/addresses/{id}/set-default/` | Yes | Set as default address |
| GET | `/api/v1/users/{id}/` | Internal only | Fetch user by auth_user_id (Order Service uses this) |
| GET | `/health/` | No | Health check |

#### Key Design Decisions
- One user can have a **maximum of 10 active addresses**
- Setting a new default address runs in a **MySQL transaction**: unset old default → set new default
- `auth_user_id` is the join key to Auth Service — User Service never calls Auth Service at read time, it trusts the JWT's `user_id` claim

---

### 6.3 Product Service

**Port:** `8003`  
**Framework:** Django 4.2 + Django REST Framework  
**Database:** MongoDB 7.0 (primary) + Redis (cache)  
**Owner:** Sayandip

#### Responsibility
The product catalog. Manages products, categories, variants, and inventory. Uses MongoDB for flexible product attributes (electronics have different attributes than clothing). Redis cache-aside pattern for high-read endpoints.

#### MongoDB Collections

**Collection: `products`**
```json
{
  "_id": "ObjectId",
  "name": "String (indexed)",
  "slug": "String (unique)",
  "description": "String",
  "price": "Decimal128",
  "sale_price": "Decimal128 (nullable)",
  "category_id": "ObjectId (ref: categories)",
  "seller_id": "UUID String",
  "brand": "String (indexed)",
  "images": [{"url": "String", "alt": "String", "is_primary": "Boolean"}],
  "tags": ["String"] ,
  "attributes": {"key": "value"},
  "is_active": "Boolean",
  "average_rating": "Float",
  "total_reviews": "Int",
  "created_at": "DateTime",
  "updated_at": "DateTime"
}
```

**Collection: `categories`**
```json
{
  "_id": "ObjectId",
  "name": "String",
  "slug": "String (unique)",
  "parent_id": "ObjectId (self-ref, null = root)",
  "level": "Int (0=root, 1=sub, 2=leaf)",
  "image_url": "String",
  "is_active": "Boolean",
  "sort_order": "Int"
}
```

**Collection: `product_variants`**
```json
{
  "_id": "ObjectId",
  "product_id": "ObjectId (ref: products)",
  "sku": "String (unique, indexed)",
  "attributes": {"color": "Red", "size": "XL"},
  "price": "Decimal128",
  "images": ["String"],
  "is_active": "Boolean"
}
```

**Collection: `inventory`**
```json
{
  "_id": "ObjectId",
  "product_id": "ObjectId",
  "variant_id": "ObjectId (nullable)",
  "sku": "String (indexed)",
  "quantity": "Int",
  "reserved": "Int",
  "available": "Int (quantity - reserved, computed)",
  "low_stock_threshold": "Int (default 10)",
  "warehouse_id": "String",
  "updated_at": "DateTime"
}
```

#### API Endpoints

| Method | Endpoint | Auth Required | Description |
|---|---|---|---|
| GET | `/api/v1/products/` | No | List products (paginated, filterable) |
| POST | `/api/v1/products/` | Yes (SELLER) | Create product |
| GET | `/api/v1/products/{id}/` | No | Get product detail |
| PATCH | `/api/v1/products/{id}/` | Yes (SELLER/ADMIN) | Update product |
| DELETE | `/api/v1/products/{id}/` | Yes (SELLER/ADMIN) | Soft-delete product |
| GET | `/api/v1/products/{id}/variants/` | No | List product variants |
| POST | `/api/v1/products/{id}/variants/` | Yes (SELLER) | Add variant |
| GET | `/api/v1/products/{id}/inventory/` | No | Get inventory status |
| PATCH | `/api/v1/products/{id}/inventory/` | Yes (SELLER) | Update inventory |
| GET | `/api/v1/categories/` | No | List category tree |
| POST | `/api/v1/categories/` | Yes (ADMIN) | Create category |
| GET | `/api/v1/categories/{id}/products/` | No | Products in category |
| GET | `/health/` | No | Health check |

#### Query Parameters for Product List
```
GET /api/v1/products/?category=electronics&brand=Samsung&min_price=500
  &max_price=50000&in_stock=true&sort=price_asc&cursor=<token>&limit=20
```

#### Caching Strategy
- **Pattern:** Cache-aside (lazy loading)
- **Key:** `product:{product_id}` → TTL 5 minutes
- **Invalidation:** On product update/delete, delete the Redis key
- **Hot path:** GET /products/{id}/ checks Redis first; on miss, fetches from MongoDB, writes to Redis

#### Kafka Events Published
- `inventory.updated` — on inventory change → consumed by Notification, Analytics

#### Elasticsearch Indexing
- Every product `save()` triggers a Django signal that indexes the product into Elasticsearch
- Indexed fields: `name`, `description`, `brand`, `tags`, `category_name`, `price`, `is_active`

---

### 6.4 Cart & Wishlist Service

**Port:** `8004`  
**Framework:** Django 4.2 + Django REST Framework  
**Database:** Redis 7.2 (100% Redis — no SQL database)  
**Owner:** Sayandip

#### Responsibility
Manages shopping carts and wishlists. Entirely Redis-backed for sub-millisecond performance and natural TTL-based expiry. Calls Product Service to validate products and snapshot prices.

#### Redis Data Structures

```
Key: cart:{user_id}                 Type: HASH
  Field: {product_id}:{variant_id}  Value: JSON
  {
    "quantity": 2,
    "price_snapshot": 999.00,
    "variant_id": "abc123",
    "product_name": "Nike Air Max 90",
    "product_image": "https://...",
    "sku": "NIKE-AM90-42",
    "added_at": "2024-01-15T10:30:00Z"
  }
  TTL: 30 days (refreshed on each access)

Key: cart_lock:{user_id}            Type: STRING
  Value: "1"
  TTL: 5 seconds (SETNX mutex for concurrent writes)

Key: wishlist:{user_id}             Type: SORTED SET
  Member: {product_id}
  Score: Unix timestamp (for newest-first ordering)
  TTL: None (wishlist doesn't expire)
```

#### API Endpoints

| Method | Endpoint | Auth Required | Description |
|---|---|---|---|
| GET | `/api/v1/cart/` | Yes | Get cart (with live prices from Product Service) |
| POST | `/api/v1/cart/items/` | Yes | Add item to cart |
| PATCH | `/api/v1/cart/items/{product_id}/` | Yes | Update item quantity |
| DELETE | `/api/v1/cart/items/{product_id}/` | Yes | Remove item |
| DELETE | `/api/v1/cart/` | Yes | Clear entire cart |
| POST | `/api/v1/cart/checkout/` | Yes | Pass cart to Order Service |
| GET | `/api/v1/wishlist/` | Yes | Get wishlist (paginated, newest first) |
| POST | `/api/v1/wishlist/items/` | Yes | Add to wishlist |
| DELETE | `/api/v1/wishlist/items/{product_id}/` | Yes | Remove from wishlist |
| POST | `/api/v1/wishlist/move-to-cart/` | Yes | Move item from wishlist to cart |
| GET | `/health/` | No | Health check |

#### Price Snapshot vs. Live Price
- **Snapshot price** is stored when item is added to cart (what the user saw at add-time)
- **Live price** is fetched from Product Service on every GET /cart/ call
- If live price differs from snapshot price, the response includes `price_changed: true` and a warning

#### Concurrent Write Protection
```python
# Acquire mutex before modifying cart
lock_acquired = redis.set(f"cart_lock:{user_id}", "1", nx=True, ex=5)
if not lock_acquired:
    raise ConcurrentModificationError("Cart is being modified, retry")
try:
    # modify cart
finally:
    redis.delete(f"cart_lock:{user_id}")
```

---

### 6.5 Order Service

**Port:** `8005`  
**Framework:** Django 4.2 + Django REST Framework  
**Database:** PostgreSQL 15  
**Owner:** Debabrata

#### Responsibility
Manages the full order lifecycle from cart checkout to delivery. Implements a strict state machine for order status transitions. Coordinates with Cart Service (read cart) and Product Service (reserve inventory). Publishes events to Kafka.

#### Database Schema

**Table: `orders`**
```sql
id                UUID          PRIMARY KEY DEFAULT gen_random_uuid()
order_number      VARCHAR(20)   UNIQUE NOT NULL  -- ORD-20240115-00001
user_id           UUID          NOT NULL  -- indexed
status            ENUM          -- see state machine below
subtotal          DECIMAL(12,2) NOT NULL
shipping_fee      DECIMAL(10,2) DEFAULT 0.00
discount          DECIMAL(10,2) DEFAULT 0.00
tax               DECIMAL(10,2) DEFAULT 0.00
total             DECIMAL(12,2) NOT NULL
shipping_address  JSONB         NOT NULL  -- snapshot at order time
coupon_code       VARCHAR(50)   NULLABLE
notes             TEXT          NULLABLE
payment_id        UUID          NULLABLE  -- set after payment
created_at        TIMESTAMPTZ   DEFAULT NOW()
updated_at        TIMESTAMPTZ   DEFAULT NOW()

INDEX: (user_id, status)
INDEX: (order_number)
INDEX: (created_at DESC)
```

**Table: `order_items`**
```sql
id             UUID          PRIMARY KEY
order_id       UUID          FK → orders.id ON DELETE CASCADE  -- indexed
product_id     VARCHAR(50)   NOT NULL  -- MongoDB ObjectId as string
variant_id     VARCHAR(50)   NULLABLE
product_name   VARCHAR(500)  NOT NULL  -- SNAPSHOT
product_image  VARCHAR(500)  NULLABLE  -- SNAPSHOT
quantity       INT           NOT NULL CHECK (quantity > 0)
unit_price     DECIMAL(10,2) NOT NULL  -- SNAPSHOT
total_price    DECIMAL(12,2) NOT NULL  -- unit_price * quantity
```

**Table: `order_status_history`**
```sql
id           UUID          PRIMARY KEY
order_id     UUID          FK → orders.id CASCADE  -- indexed
from_status  ENUM          NULLABLE  -- NULL on creation
to_status    ENUM          NOT NULL
changed_by   UUID          NULLABLE  -- NULL = system
reason       TEXT          NULLABLE
changed_at   TIMESTAMPTZ   DEFAULT NOW()
```

**Table: `delivery_tracking`**
```sql
id                  UUID          PRIMARY KEY
order_id            UUID          FK → orders.id UNIQUE
carrier_name        VARCHAR(100)  -- e.g. Delhivery
tracking_number     VARCHAR(100)  UNIQUE
tracking_url        VARCHAR(500)  NULLABLE
estimated_delivery  DATE          NULLABLE
actual_delivery     TIMESTAMPTZ   NULLABLE
```

#### Order State Machine

```
                         ┌─────────────┐
                         │   PENDING   │ ← Order created
                         └──────┬──────┘
                                │ (payment initiated)
                    ┌───────────▼─────────────┐
                    │   PAYMENT_PENDING        │
                    └───────────┬─────────────┘
                                │ (payment.success Kafka event)
                    ┌───────────▼─────────────┐
                    │      CONFIRMED           │
                    └───────────┬─────────────┘
                                │ (seller accepts)
                    ┌───────────▼─────────────┐
                    │      PROCESSING          │
                    └───────────┬─────────────┘
                                │ (items packed)
                    ┌───────────▼─────────────┐
                    │        PACKED            │
                    └───────────┬─────────────┘
                                │ (shipped)
                    ┌───────────▼─────────────┐
                    │        SHIPPED           │
                    └───────────┬─────────────┘
                                │ (delivered)
                    ┌───────────▼─────────────┐
                    │       DELIVERED          │
                    └───────────┬─────────────┘
                                │ (return requested)
                    ┌───────────▼─────────────┐
                    │   RETURN_REQUESTED       │
                    └───────┬─────────┬────────┘
                            │         │
                   ┌────────▼──┐  ┌───▼──────────────┐
                   │ RETURNED  │  │ RETURN_REJECTED   │
                   └───────────┘  └──────────────────┘

CANCELLED ← from PENDING or CONFIRMED only
```

#### API Endpoints

| Method | Endpoint | Auth Required | Description |
|---|---|---|---|
| POST | `/api/v1/orders/` | Yes | Create order from cart |
| GET | `/api/v1/orders/` | Yes | List user's orders (paginated) |
| GET | `/api/v1/orders/{id}/` | Yes | Get order detail |
| POST | `/api/v1/orders/{id}/cancel/` | Yes | Cancel order |
| POST | `/api/v1/orders/{id}/return/` | Yes | Request return (DELIVERED only) |
| GET | `/api/v1/orders/{id}/tracking/` | Yes | Get delivery tracking |
| PATCH | `/api/v1/orders/{id}/status/` | Yes (ADMIN) | Admin status update |
| GET | `/health/` | No | Health check |

#### Kafka Events Published
- `order.created` — on order creation
- `order.confirmed` — on payment confirmed
- `order.cancelled` — on cancellation
- `order.status_updated` — on every status transition
- `order.delivered` — on delivery (triggers loyalty points etc.)

---

### 6.6 Payment Service

**Port:** `8006`  
**Framework:** Django 4.2 + Django REST Framework  
**Database:** MySQL 8.0  
**Owner:** Debabrata

#### Responsibility
Handles all payment initiation, verification, and refund processing. Integrates with Razorpay (primary) and Stripe (secondary) via an adapter pattern so gateways can be swapped. Webhook security via HMAC-SHA256 signature verification.

#### Database Schema

**Table: `payments`**
```sql
id                  CHAR(36)     PRIMARY KEY
payment_ref         VARCHAR(30)  UNIQUE NOT NULL  -- PAY-20240115-00001
order_id            CHAR(36)     UNIQUE NOT NULL  -- indexed
user_id             CHAR(36)     NOT NULL         -- indexed
amount              DECIMAL(12,2) NOT NULL CHECK (amount > 0)
currency            VARCHAR(3)   DEFAULT 'INR'
method              ENUM         CARD|UPI|NETBANKING|COD|WALLET
status              ENUM         INITIATED|PENDING|SUCCESS|FAILED|REFUNDED|PARTIALLY_REFUNDED
gateway             ENUM         RAZORPAY|STRIPE|COD|PAYTM
gateway_txn_id      VARCHAR(200) UNIQUE NULLABLE  -- set after gateway responds
gateway_order_id    VARCHAR(200) NULLABLE
gateway_response    JSON         NULLABLE         -- full raw response stored
failure_reason      VARCHAR(500) NULLABLE
paid_at             DATETIME     NULLABLE
created_at          DATETIME     DEFAULT CURRENT_TIMESTAMP
updated_at          DATETIME     ON UPDATE CURRENT_TIMESTAMP
```

**Table: `refunds`**
```sql
id                  CHAR(36)     PRIMARY KEY
payment_id          CHAR(36)     FK → payments.id  -- indexed
refund_ref          VARCHAR(30)  UNIQUE NOT NULL
amount              DECIMAL(10,2) NOT NULL
reason              ENUM         CUSTOMER_REQUEST|ORDER_CANCELLED|FRAUD|DEFECTIVE
notes               TEXT         NULLABLE
status              ENUM         PENDING|PROCESSING|PROCESSED|FAILED
gateway_refund_id   VARCHAR(200) NULLABLE
initiated_by        CHAR(36)     NULLABLE  -- user_id, NULL = system
initiated_at        DATETIME     DEFAULT CURRENT_TIMESTAMP
processed_at        DATETIME     NULLABLE
```

**Table: `payment_methods`**
```sql
id               CHAR(36)     PRIMARY KEY
user_id          CHAR(36)     NOT NULL  -- indexed
type             ENUM         CARD|UPI|WALLET
display_name     VARCHAR(100) -- e.g. "HDFC •••• 4242"
masked_number    VARCHAR(20)  NULLABLE
expiry_month     INT          NULLABLE
expiry_year      INT          NULLABLE
gateway_token    VARCHAR(500) -- AES-256 encrypted
is_default       BOOLEAN      DEFAULT FALSE
is_active        BOOLEAN      DEFAULT TRUE
created_at       DATETIME     DEFAULT CURRENT_TIMESTAMP
```

#### API Endpoints

| Method | Endpoint | Auth Required | Description |
|---|---|---|---|
| POST | `/api/v1/payments/initiate/` | Yes | Initiate payment for an order |
| GET | `/api/v1/payments/{id}/status/` | Yes | Poll payment status |
| POST | `/api/v1/payments/webhook/` | **No JWT** (IP whitelist) | Razorpay/Stripe webhook |
| POST | `/api/v1/payments/{id}/refund/` | Yes (ADMIN) | Initiate refund |
| GET | `/api/v1/payments/history/` | Yes | User's payment history |
| GET | `/api/v1/payments/methods/` | Yes | List saved payment methods |
| POST | `/api/v1/payments/methods/` | Yes | Save payment method |
| DELETE | `/api/v1/payments/methods/{id}/` | Yes | Remove saved method |
| GET | `/health/` | No | Health check |

#### Gateway Adapter Pattern
```python
class GatewayAdapter(ABC):
    @abstractmethod
    def create_order(self, amount, currency, order_id): ...
    @abstractmethod
    def verify_payment(self, payment_id, signature): ...
    @abstractmethod
    def initiate_refund(self, payment_id, amount, reason): ...

class RazorpayAdapter(GatewayAdapter): ...
class StripeAdapter(GatewayAdapter): ...
class CODAdapter(GatewayAdapter): ...  # Cash on delivery mock
```

#### Webhook Security
```python
# NEVER update payment status from client-side — only from webhook
# Verify HMAC-SHA256 signature before processing
import hmac, hashlib

def verify_razorpay_signature(order_id, payment_id, signature, secret):
    body = f"{order_id}|{payment_id}"
    expected = hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)
```

#### Kafka Events Published
- `payment.success` — consumed by Order Service, Notification Service, Analytics
- `payment.failed` — consumed by Order Service, Notification Service
- `payment.refund_processed` — consumed by Notification Service

---

### 6.7 Search Service

**Port:** `8007`  
**Framework:** FastAPI 0.104+  
**Database:** Elasticsearch 8.x  
**Owner:** Sayandip

#### Responsibility
Full-text product search with autocomplete, faceted filtering, relevance scoring, and cursor-based pagination. Indexed data comes from Product Service via Django signals.

#### Elasticsearch Index: `products`
```json
{
  "mappings": {
    "properties": {
      "product_id":     {"type": "keyword"},
      "name":           {"type": "text", "analyzer": "standard"},
      "description":    {"type": "text", "analyzer": "standard"},
      "brand":          {"type": "keyword"},
      "category_name":  {"type": "keyword"},
      "tags":           {"type": "keyword"},
      "price":          {"type": "float"},
      "sale_price":     {"type": "float"},
      "is_active":      {"type": "boolean"},
      "average_rating": {"type": "float"},
      "suggest":        {"type": "completion"}
    }
  }
}
```

#### API Endpoints

| Method | Endpoint | Auth | Description |
|---|---|---|---|
| GET | `/api/v1/search/products/` | No | Full-text search with filters |
| GET | `/api/v1/search/autocomplete/` | No | Autocomplete suggestions |
| GET | `/api/v1/search/filters/` | No | Get available filter facets |
| POST | `/api/v1/search/index/` | Internal | Index/re-index a product |
| DELETE | `/api/v1/search/index/{id}/` | Internal | Remove product from index |
| GET | `/health/` | No | Health check |

#### Query Parameters
```
GET /api/v1/search/products/?q=samsung+phone
  &category=smartphones
  &brand=Samsung
  &min_price=10000
  &max_price=80000
  &in_stock=true
  &min_rating=4
  &sort=relevance|price_asc|price_desc|newest|rating
  &cursor=<base64_token>
  &limit=20
```

---

### 6.8 AI Recommendation Service

**Port:** `8008`  
**Framework:** FastAPI 0.104+  
**Database:** Qdrant 1.x (Vector DB)  
**Owner:** Sayandip

#### Responsibility
Generates product recommendations using vector embeddings. Products are embedded using `sentence-transformers` and stored in Qdrant. Recommendations are retrieved via cosine similarity search.

#### How It Works
1. Product Service calls `/embed/` endpoint when a product is created/updated
2. Service generates a vector embedding from `name + description + brand + tags`
3. Embedding stored in Qdrant collection `products` with product_id as payload
4. Recommendation endpoint takes `product_id` → finds nearest neighbours → returns similar products

#### API Endpoints

| Method | Endpoint | Auth | Description |
|---|---|---|---|
| GET | `/api/v1/recommendations/{product_id}/` | No | Get similar products |
| GET | `/api/v1/recommendations/user/{user_id}/` | Yes | Personalised recommendations |
| POST | `/api/v1/embed/product/` | Internal | Generate + store product embedding |
| DELETE | `/api/v1/embed/product/{id}/` | Internal | Remove product embedding |
| GET | `/health/` | No | Health check |

---

### 6.9 Notification Service

**Port:** `8009`  
**Framework:** FastAPI 0.104+ (async Kafka consumer — no REST API Gateway routes)  
**Database:** MongoDB (notification logs + templates)  
**Owner:** Debabrata

#### Responsibility
Pure event-driven service. Listens to Kafka topics and sends email/SMS/push notifications. Has no public REST endpoints — the API Gateway does not route to it. Notification templates are stored in MongoDB.

#### Kafka Topics Consumed
| Topic | Trigger | Notification |
|---|---|---|
| `user.registered` | New registration | Welcome email + verification email |
| `order.created` | Order placed | Order confirmation email |
| `order.confirmed` | Payment success | Payment confirmed email |
| `order.shipped` | Order shipped | Shipping notification with tracking |
| `order.delivered` | Order delivered | Delivery confirmation, review request |
| `order.cancelled` | Order cancelled | Cancellation confirmation |
| `payment.failed` | Payment failed | Payment failure alert |
| `payment.refund_processed` | Refund done | Refund confirmation |

#### MongoDB Collection: `notification_logs`
```json
{
  "_id": "ObjectId",
  "user_id": "UUID",
  "type": "EMAIL|SMS|PUSH",
  "template": "order_confirmation",
  "subject": "Your order #ORD-20240115 is confirmed",
  "status": "SENT|FAILED|PENDING",
  "sent_at": "DateTime",
  "error": "String (if failed)"
}
```

---

### 6.10 Analytics Service

**Port:** `8010`  
**Framework:** FastAPI 0.104+  
**Database:** ClickHouse 23.x  
**Owner:** Debabrata

#### Responsibility
Consumes all Kafka events and writes aggregated analytics to ClickHouse. Exposes read endpoints for dashboard queries. ClickHouse's columnar storage makes aggregations like "total revenue today" extremely fast.

#### API Endpoints

| Method | Endpoint | Auth | Description |
|---|---|---|---|
| GET | `/api/v1/analytics/revenue/` | ADMIN | Revenue by period |
| GET | `/api/v1/analytics/top-products/` | ADMIN | Best selling products |
| GET | `/api/v1/analytics/orders/` | ADMIN | Order stats |
| GET | `/api/v1/analytics/users/` | ADMIN | User registration trends |
| GET | `/health/` | No | Health check |

---

### 6.11 WebSocket Gateway

**Port:** `8011`  
**Framework:** Django Channels 4.x  
**Channel Layer:** Redis 7.2  
**Owner:** Debabrata

#### Responsibility
Real-time order and payment status updates pushed to the browser. Uses Django Channels with Redis as the channel layer. When an order status changes, the Order Service publishes a Kafka event, a worker consumes it and pushes to the Redis channel, and Django Channels delivers it to the connected WebSocket client.

#### Connection URL
```
wss://api.shopsphere.com/ws/orders/{order_id}/
Authorization: Bearer <access_token>   (sent as query param or header)
```

#### Message Format (Server → Client)
```json
{
  "type": "order_status_update",
  "order_id": "uuid",
  "new_status": "SHIPPED",
  "message": "Your order has been shipped!",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

---

### 6.12 GraphQL Gateway

**Port:** `8012`  
**Framework:** Strawberry GraphQL  
**Owner:** Shared (Sayandip + Debabrata)

#### Responsibility
A thin GraphQL layer wrapping User Service and Product Service. Allows clients to fetch user profile + order history + recommended products in a single query instead of three separate REST calls.

#### Example Query
```graphql
query GetUserDashboard($userId: ID!) {
  user(id: $userId) {
    firstName
    lastName
    email
    orders(limit: 5) {
      id
      orderNumber
      status
      total
      createdAt
    }
  }
  recommendations(userId: $userId, limit: 10) {
    id
    name
    price
    averageRating
  }
}
```

#### Endpoint
```
POST /graphql
Content-Type: application/json
Authorization: Bearer <access_token>
```

---

## 7. Kafka Event Architecture

### Topics and Partitions

| Topic | Partitions | Replication | Key | Producers | Consumers |
|---|---|---|---|---|---|
| `user.registered` | 3 | 1 | user_id | Auth Service | Notification |
| `order.created` | 6 | 1 | order_id | Order Service | Notification, Analytics |
| `order.confirmed` | 6 | 1 | order_id | Order Service | Notification |
| `order.cancelled` | 3 | 1 | order_id | Order Service | Notification, Analytics, Inventory |
| `order.shipped` | 3 | 1 | order_id | Order Service | Notification |
| `order.delivered` | 3 | 1 | order_id | Order Service | Notification, Analytics |
| `order.status_updated` | 6 | 1 | order_id | Order Service | Analytics, WebSocket |
| `payment.success` | 6 | 1 | order_id | Payment Service | Order, Notification, Analytics |
| `payment.failed` | 3 | 1 | order_id | Payment Service | Order, Notification |
| `payment.refund_processed` | 3 | 1 | payment_id | Payment Service | Notification |
| `inventory.updated` | 3 | 1 | product_id | Product Service | Analytics, Notification |

### Event Payload Structure (Standard)
```json
{
  "event_id": "uuid-v4",
  "event_type": "order.created",
  "version": "1.0",
  "timestamp": "2024-01-15T10:30:00Z",
  "source_service": "order-service",
  "correlation_id": "req-uuid",
  "payload": {
    ...event-specific data...
  }
}
```

### Consumer Groups

| Consumer Group | Services | Behaviour |
|---|---|---|
| `notification-group` | Notification Service | Each event processed once |
| `analytics-group` | Analytics Service | Each event processed once |
| `order-group` | Order Service | Listens to payment events |
| `inventory-group` | Product Service | Listens to order.cancelled |

---

## 8. NGINX & Load Balancer

### nginx.conf Structure
```nginx
# Rate limiting zone
limit_req_zone $binary_remote_addr zone=api:10m rate=100r/m;

# Upstream pools
upstream django_services {
    server django_1:8001;
    server django_2:8001;
    server django_3:8001;
    keepalive 32;
}

upstream fastapi_services {
    server fastapi_1:8007;
    server fastapi_2:8007;
    keepalive 16;
}

server {
    listen 443 ssl http2;

    # SSL
    ssl_certificate     /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;

    # Rate limiting
    limit_req zone=api burst=20 nodelay;

    # Route by path prefix
    location /api/v1/auth/      { proxy_pass http://auth_service:8001; }
    location /api/v1/users/     { proxy_pass http://user_service:8002; }
    location /api/v1/products/  { proxy_pass http://product_service:8003; }
    location /api/v1/cart/      { proxy_pass http://cart_service:8004; }
    location /api/v1/wishlist/  { proxy_pass http://cart_service:8004; }
    location /api/v1/orders/    { proxy_pass http://order_service:8005; }
    location /api/v1/payments/  { proxy_pass http://payment_service:8006; }
    location /api/v1/search/    { proxy_pass http://search_service:8007; }
    location /api/v1/recommend/ { proxy_pass http://ai_service:8008; }
    location /graphql           { proxy_pass http://graphql_gateway:8012; }

    # WebSocket
    location /ws/ {
        proxy_pass http://websocket_gateway:8011;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}

server {
    listen 80;
    return 301 https://$host$request_uri;
}
```

---

## 9. WebSocket Architecture

```
Browser                NGINX              Django Channels       Redis             Kafka
   │                     │                      │                  │                │
   │──ws://orders/123───▶│                      │                  │                │
   │                     │──upgrade──────────▶  │                  │                │
   │                     │                      │──subscribe───────▶                │
   │                     │                      │  channel:order_123                │
   │                     │                      │                  │                │
   │                     │                      │                  │◀──order.status─│
   │                     │                      │◀──message────────│  _updated event│
   │◀────status update───│◀─────push────────────│                  │                │
```

---

## 10. Elasticsearch Integration

### Indexing Pipeline
```
Product Service (save signal)
        │
        ▼
ES Indexer (celery task or sync)
        │
        ▼
POST /products/_doc/{id}
{
  "name": "Samsung Galaxy S24",
  "brand": "Samsung",
  "category_name": "Smartphones",
  "price": 74999,
  "tags": ["android", "5G", "samsung"],
  "suggest": {"input": ["Samsung Galaxy", "Galaxy S24"]}
}
```

### Search Query Flow
```python
# Example Elasticsearch query
query = {
    "bool": {
        "must": [{"multi_match": {"query": q, "fields": ["name^3", "description", "brand^2", "tags"]}}],
        "filter": [
            {"term": {"is_active": True}},
            {"range": {"price": {"gte": min_price, "lte": max_price}}},
            {"term": {"category_name": category}}
        ]
    }
}
```

### Autocomplete
```
GET /api/v1/search/autocomplete/?q=sam
→ ["Samsung Galaxy S24", "Samsung Galaxy A55", "Samsung TV 55 inch"]
```

---

## 11. CI/CD Pipeline

### Pipeline Stages (GitHub Actions)

```yaml
name: CI/CD Pipeline

on:
  push:
    branches: [main, staging]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install flake8 black isort
      - run: flake8 services/
      - run: black --check services/
      - run: isort --check services/

  test:
    runs-on: ubuntu-latest
    services:
      postgres: {image: postgres:15}
      mysql: {image: mysql:8.0}
      redis: {image: redis:7.2}
    steps:
      - run: pytest services/ --cov=. --cov-report=xml

  build:
    needs: [lint, test]
    runs-on: ubuntu-latest
    steps:
      - run: docker compose build
      - run: docker push ghcr.io/$GITHUB_REPOSITORY/auth-service:$GITHUB_SHA

  integration-test:
    needs: build
    steps:
      - run: docker compose up -d
      - run: pytest tests/integration/ --timeout=60

  deploy-staging:
    needs: integration-test
    if: github.ref == 'refs/heads/staging'
    steps:
      - run: kubectl set image deployment/auth-service ...

  deploy-prod:
    needs: deploy-staging
    if: github.ref == 'refs/heads/main'
    environment: production
    steps:
      - run: kubectl set image deployment/auth-service ...
```

### Branch Protection Rules
- `main` → requires PR review + all CI passing + integration tests passing
- `staging` → requires PR review + unit tests passing
- Feature branches → lint + unit tests only

---

## 12. Getting Started

### Prerequisites

Ensure you have the following installed:
```bash
docker --version        # Docker 24.0+
docker compose version  # Docker Compose 2.20+
git --version           # Git 2.40+
python --version        # Python 3.11+ (for local dev only)
```

### 1. Clone the Repository
```bash
git clone https://github.com/your-org/shopsphere-backend.git
cd shopsphere-backend
```

### 2. Configure Environment Variables
```bash
cp .env.example .env
# Edit .env with your actual values
nano .env
```

### 3. Start All Services
```bash
# Start all infrastructure first (databases, Kafka, Redis, Elasticsearch)
docker compose up -d postgres mysql mongodb redis kafka zookeeper elasticsearch qdrant clickhouse

# Wait for DBs to be ready (about 30 seconds)
sleep 30

# Start all application services
docker compose up -d

# Check all services are running
docker compose ps
```

### 4. Run Migrations
```bash
# Auth Service
docker compose exec auth-service python manage.py migrate

# User Service
docker compose exec user-service python manage.py migrate

# Order Service
docker compose exec order-service python manage.py migrate

# Payment Service
docker compose exec payment-service python manage.py migrate
```

### 5. Create a Superuser (for Admin)
```bash
docker compose exec auth-service python manage.py createsuperuser
```

### 6. Seed Test Data (Optional)
```bash
docker compose exec product-service python manage.py seed_products --count=100
docker compose exec user-service python manage.py seed_users --count=20
```

### 7. Access the Services

| Service | URL | Description |
|---|---|---|
| Auth Service | http://localhost:8001/api/v1/ | Authentication endpoints |
| User Service | http://localhost:8002/api/v1/ | User profile endpoints |
| Product Service | http://localhost:8003/api/v1/ | Product catalog |
| Cart Service | http://localhost:8004/api/v1/ | Cart management |
| Order Service | http://localhost:8005/api/v1/ | Order management |
| Payment Service | http://localhost:8006/api/v1/ | Payment processing |
| Search Service | http://localhost:8007/api/v1/ | Product search |
| AI Service | http://localhost:8008/api/v1/ | Recommendations |
| Via NGINX | http://localhost:80/ | All services unified |
| Auth Swagger | http://localhost:8001/api/schema/swagger-ui/ | API docs |
| Kafka UI | http://localhost:9000/ | Kafka topic browser |

### Quick Smoke Test
```bash
# Register a user
curl -X POST http://localhost/api/v1/auth/register/ \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com", "password": "SecurePass123!", "role": "BUYER"}'

# Login
curl -X POST http://localhost/api/v1/auth/login/ \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com", "password": "SecurePass123!"}'

# Browse products (no auth needed)
curl http://localhost/api/v1/products/
```

---

## 13. Environment Variables

Create a `.env` file in the project root. Copy from `.env.example`:

```bash
# ─── SHARED ───────────────────────────────────────────────────
DEBUG=False
SECRET_KEY=your-super-secret-django-key-min-50-chars
ALLOWED_HOSTS=localhost,127.0.0.1,0.0.0.0
CORS_ALLOWED_ORIGINS=http://localhost:3000

# ─── AUTH SERVICE ─────────────────────────────────────────────
AUTH_DB_HOST=postgres
AUTH_DB_PORT=5432
AUTH_DB_NAME=auth_db
AUTH_DB_USER=auth_user
AUTH_DB_PASSWORD=auth_password
JWT_ACCESS_TOKEN_LIFETIME_MINUTES=15
JWT_REFRESH_TOKEN_LIFETIME_DAYS=7
JWT_SIGNING_KEY=your-jwt-signing-key

# ─── USER SERVICE ─────────────────────────────────────────────
USER_DB_HOST=mysql
USER_DB_PORT=3306
USER_DB_NAME=user_db
USER_DB_USER=user_user
USER_DB_PASSWORD=user_password

# ─── PRODUCT SERVICE ──────────────────────────────────────────
MONGO_HOST=mongodb
MONGO_PORT=27017
MONGO_DB_NAME=product_db
MONGO_USER=product_user
MONGO_PASSWORD=product_password

# ─── REDIS ────────────────────────────────────────────────────
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=redis_password
REDIS_CART_DB=0
REDIS_CACHE_DB=1
REDIS_BLACKLIST_DB=2
REDIS_CHANNELS_DB=3

# ─── ORDER SERVICE ────────────────────────────────────────────
ORDER_DB_HOST=postgres
ORDER_DB_PORT=5432
ORDER_DB_NAME=order_db
ORDER_DB_USER=order_user
ORDER_DB_PASSWORD=order_password

# ─── PAYMENT SERVICE ──────────────────────────────────────────
PAYMENT_DB_HOST=mysql
PAYMENT_DB_PORT=3306
PAYMENT_DB_NAME=payment_db
PAYMENT_DB_USER=payment_user
PAYMENT_DB_PASSWORD=payment_password
RAZORPAY_KEY_ID=rzp_test_xxxxxxxxxxxx
RAZORPAY_KEY_SECRET=your-razorpay-secret
STRIPE_SECRET_KEY=sk_test_xxxxxxxxxxxx
STRIPE_WEBHOOK_SECRET=whsec_xxxxxxxxxxxx

# ─── KAFKA ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_GROUP_ID_NOTIFICATION=notification-group
KAFKA_GROUP_ID_ANALYTICS=analytics-group

# ─── ELASTICSEARCH ────────────────────────────────────────────
ELASTICSEARCH_HOST=elasticsearch
ELASTICSEARCH_PORT=9200
ELASTICSEARCH_INDEX_PRODUCTS=products

# ─── QDRANT ───────────────────────────────────────────────────
QDRANT_HOST=qdrant
QDRANT_PORT=6333
QDRANT_COLLECTION_PRODUCTS=products
EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2

# ─── CLICKHOUSE ───────────────────────────────────────────────
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB=analytics
CLICKHOUSE_USER=analytics_user
CLICKHOUSE_PASSWORD=analytics_password

# ─── EMAIL (NOTIFICATIONS) ────────────────────────────────────
SENDGRID_API_KEY=SG.xxxxxxxxxxxx
FROM_EMAIL=noreply@shopsphere.com
```

> ⚠️ **NEVER commit your `.env` file.** It is in `.gitignore`. Use GitHub Secrets for CI/CD.

---

## 14. Docker Compose Reference

```yaml
# docker-compose.yml (abbreviated — full file in repo)
version: '3.9'

services:
  # ── Databases ──────────────────────────────────────
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_MULTIPLE_DATABASES: auth_db,order_db
    volumes: [postgres_data:/var/lib/postgresql/data]
    ports: ["5432:5432"]

  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: ${MYSQL_ROOT_PASSWORD}
    volumes: [mysql_data:/var/lib/mysql]
    ports: ["3306:3306"]

  mongodb:
    image: mongo:7.0
    volumes: [mongo_data:/data/db]
    ports: ["27017:27017"]

  redis:
    image: redis:7.2-alpine
    command: redis-server --requirepass ${REDIS_PASSWORD}
    ports: ["6379:6379"]

  elasticsearch:
    image: elasticsearch:8.11.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
    volumes: [es_data:/usr/share/elasticsearch/data]
    ports: ["9200:9200"]

  qdrant:
    image: qdrant/qdrant:latest
    volumes: [qdrant_data:/qdrant/storage]
    ports: ["6333:6333"]

  clickhouse:
    image: clickhouse/clickhouse-server:23-alpine
    volumes: [clickhouse_data:/var/lib/clickhouse]
    ports: ["9000:9000", "8123:8123"]

  # ── Kafka ──────────────────────────────────────────
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    ports: ["9092:9092"]

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    ports: ["9000:8080"]
    environment:
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092

  # ── Application Services ───────────────────────────
  auth-service:
    build: ./services/auth-service
    env_file: .env
    depends_on: [postgres, redis]
    ports: ["8001:8001"]

  nginx:
    image: nginx:1.25-alpine
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf
      - ./nginx/ssl:/etc/nginx/ssl
    ports: ["80:80", "443:443"]
    depends_on: [auth-service, user-service, product-service]

volumes:
  postgres_data:
  mysql_data:
  mongo_data:
  es_data:
  qdrant_data:
  clickhouse_data:
```

### Useful Docker Commands

```bash
# Start everything
docker compose up -d

# View logs for a specific service
docker compose logs -f auth-service

# Restart a single service
docker compose restart product-service

# Scale a service to 3 instances
docker compose up -d --scale product-service=3

# Run a Django shell
docker compose exec auth-service python manage.py shell

# Run tests for a service
docker compose exec auth-service pytest

# Stop everything
docker compose down

# Stop and remove all volumes (WARNING: deletes all data)
docker compose down -v
```

---

## 15. API Documentation

Each service auto-generates OpenAPI documentation using `drf-spectacular` (Django) or FastAPI's built-in OpenAPI support.

| Service | Swagger UI | ReDoc |
|---|---|---|
| Auth | http://localhost:8001/api/schema/swagger-ui/ | http://localhost:8001/api/schema/redoc/ |
| User | http://localhost:8002/api/schema/swagger-ui/ | http://localhost:8002/api/schema/redoc/ |
| Product | http://localhost:8003/api/schema/swagger-ui/ | http://localhost:8003/api/schema/redoc/ |
| Cart | http://localhost:8004/api/schema/swagger-ui/ | http://localhost:8004/api/schema/redoc/ |
| Order | http://localhost:8005/api/schema/swagger-ui/ | http://localhost:8005/api/schema/redoc/ |
| Payment | http://localhost:8006/api/schema/swagger-ui/ | http://localhost:8006/api/schema/redoc/ |
| Search | http://localhost:8007/docs | http://localhost:8007/redoc |
| AI | http://localhost:8008/docs | http://localhost:8008/redoc |

### Standard Response Format

**Success Response:**
```json
{
  "status": "success",
  "data": { ... },
  "meta": {
    "pagination": {
      "next_cursor": "eyJpZCI6IjEwMCJ9",
      "has_next": true,
      "count": 20
    }
  }
}
```

**Error Response:**
```json
{
  "status": "error",
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Email address is already registered.",
    "field": "email"
  }
}
```

### Pagination (Cursor-Based)
All list endpoints use **cursor-based pagination** (not offset/limit):
```
GET /api/v1/products/?limit=20&cursor=eyJpZCI6IjEwMCJ9

Response:
{
  "results": [...],
  "next_cursor": "eyJpZCI6IjEyMCJ9",
  "has_next": true
}
```

Why cursor-based over offset? With offset pagination, if items are added/removed between pages, you get duplicates or skip items. Cursor-based pagination is stable and performs better on large datasets.

---

## 16. Folder Structure

```
shopsphere-backend/
│
├── docker-compose.yml              # All services + databases
├── docker-compose.test.yml         # Integration test overrides
├── .env.example                    # Template for environment variables
├── .gitignore
├── README.md                       # ← You are here
│
├── nginx/
│   ├── nginx.conf                  # Main NGINX config (reverse proxy + LB)
│   └── ssl/                        # SSL certificates (gitignored)
│
├── kafka/
│   └── topics.yml                  # Kafka topic definitions
│
├── .github/
│   └── workflows/
│       ├── ci.yml                  # Lint + unit tests on every PR
│       ├── integration.yml         # Integration tests on merge to staging
│       └── deploy.yml              # Deploy on merge to main
│
├── services/
│   │
│   ├── auth-service/               # 🟡 SAYANDIP
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── manage.py
│   │   ├── config/
│   │   │   ├── settings.py
│   │   │   ├── urls.py
│   │   │   └── wsgi.py
│   │   └── auth/
│   │       ├── models.py           # AuthUser, RefreshToken, EmailVerification
│   │       ├── views.py            # AuthViewSet
│   │       ├── serializers.py
│   │       ├── urls.py
│   │       ├── services/
│   │       │   ├── user_manager.py
│   │       │   ├── jwt_utils.py
│   │       │   └── token_service.py
│   │       ├── kafka/
│   │       │   └── producer.py
│   │       └── tests/
│   │           ├── test_register.py
│   │           ├── test_login.py
│   │           └── test_token_refresh.py
│   │
│   ├── user-service/               # 🔵 DEBABRATA
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── users/
│   │       ├── models.py           # UserProfile, Address
│   │       ├── views.py
│   │       ├── serializers.py
│   │       ├── services/
│   │       │   ├── user_service.py
│   │       │   └── address_service.py
│   │       └── tests/
│   │
│   ├── product-service/            # 🟡 SAYANDIP
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── products/
│   │       ├── models.py           # Product, Category, Inventory (MongoEngine)
│   │       ├── views.py
│   │       ├── serializers.py
│   │       ├── pagination.py       # CursorPagination
│   │       ├── filters.py          # DjangoFilterBackend config
│   │       ├── services/
│   │       │   ├── product_service.py
│   │       │   ├── inventory_manager.py
│   │       │   └── cache_manager.py
│   │       ├── signals.py          # ES indexing on save
│   │       ├── kafka/
│   │       │   └── producer.py
│   │       └── tests/
│   │
│   ├── cart-service/               # 🟡 SAYANDIP
│   │   └── cart/
│   │       ├── views.py
│   │       ├── services/
│   │       │   ├── cart_service.py
│   │       │   ├── wishlist_service.py
│   │       │   └── checkout_service.py
│   │       └── clients/
│   │           └── product_client.py   # HTTP client for Product Service
│   │
│   ├── order-service/              # 🔵 DEBABRATA
│   │   └── orders/
│   │       ├── models.py           # Order, OrderItem, StatusHistory, Tracking
│   │       ├── state_machine.py    # OrderStateMachine
│   │       ├── services/
│   │       │   ├── order_service.py
│   │       │   └── tracking_service.py
│   │       ├── kafka/
│   │       │   └── producer.py
│   │       └── clients/
│   │           ├── cart_client.py
│   │           └── product_client.py
│   │
│   ├── payment-service/            # 🔵 DEBABRATA
│   │   └── payments/
│   │       ├── models.py           # Payment, Refund, PaymentMethod
│   │       ├── views.py            # PaymentViewSet, WebhookView
│   │       ├── gateways/
│   │       │   ├── base.py         # GatewayAdapter ABC
│   │       │   ├── razorpay.py
│   │       │   └── stripe.py
│   │       ├── kafka/
│   │       │   └── producer.py
│   │       └── tests/
│   │
│   ├── search-service/             # 🟡 SAYANDIP (FastAPI)
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── main.py
│   │   └── app/
│   │       ├── routers/
│   │       │   └── search.py
│   │       ├── schemas/
│   │       │   └── search.py
│   │       ├── es_client.py
│   │       └── tests/
│   │
│   ├── ai-recommendation/          # 🟡 SAYANDIP (FastAPI)
│   │   ├── main.py
│   │   └── app/
│   │       ├── embeddings.py
│   │       └── qdrant_client.py
│   │
│   ├── notification-service/       # 🔵 DEBABRATA (FastAPI)
│   │   ├── main.py
│   │   └── app/
│   │       ├── kafka/
│   │       │   └── consumer.py
│   │       └── senders/
│   │           ├── email.py
│   │           └── sms.py
│   │
│   ├── analytics-service/          # 🔵 DEBABRATA (FastAPI)
│   │   └── (same FastAPI structure)
│   │
│   └── websocket-service/          # 🔵 DEBABRATA (Django Channels)
│       └── ws/
│           ├── consumers.py
│           └── routing.py
│
├── graphql-gateway/                # 🟢 SHARED
│   ├── schema.py
│   └── resolvers/
│
└── tests/
    └── integration/                # End-to-end tests across services
        ├── test_checkout_flow.py
        ├── test_payment_flow.py
        └── conftest.py
```

---

## 17. Development Workflow

### Git Strategy

```
main          ← production, protected, requires PR + CI passing
  └── staging ← integration environment
        ├── feature/auth-jwt-refresh       (Sayandip)
        ├── feature/order-state-machine    (Debabrata)
        └── fix/cart-redis-ttl             (either)
```

### Branch Naming Convention
```
feature/<service>-<description>   →  feature/product-cursor-pagination
fix/<service>-<description>       →  fix/auth-refresh-token-rotation
chore/<description>               →  chore/update-kafka-client-version
test/<description>                →  test/order-integration-suite
```

### Pull Request Checklist
Before opening a PR, ensure:
- [ ] All unit tests pass (`pytest`)
- [ ] Code is linted (`flake8 .`)
- [ ] Code is formatted (`black .`)
- [ ] Imports are sorted (`isort .`)
- [ ] New environment variables are added to `.env.example`
- [ ] Database migrations are included if schema changed
- [ ] OpenAPI spec is updated if endpoints changed
- [ ] At least 1 reviewer assigned (the other team member)

### Commit Message Convention
```
feat(auth): add refresh token rotation
fix(cart): resolve race condition on concurrent cart updates
docs(order): update state machine documentation
test(payment): add webhook signature validation tests
chore(docker): upgrade postgres to 15.4
refactor(product): extract cache logic into CacheManager class
```

### Code Style
- **Python:** PEP 8, enforced by `flake8` + `black`
- **Max line length:** 119 characters
- **Imports:** sorted by `isort` with `django` as known third-party
- **Docstrings:** Google-style for all public methods
- **Type hints:** required on all function signatures

---

## 18. Testing Strategy

### Test Pyramid

```
                    ┌─────────────┐
                    │  E2E Tests  │  ← Full flow (Checkout → Payment → Notification)
                    │   (Slow)    │     5–10 tests
                    └──────┬──────┘
               ┌───────────┴───────────┐
               │   Integration Tests   │  ← Service API with real DB
               │      (Medium)         │     20–30 per service
               └───────────┬───────────┘
          ┌─────────────────┴──────────────────┐
          │           Unit Tests               │  ← Business logic, serializers
          │            (Fast)                  │     50–100 per service
          └────────────────────────────────────┘
```

### Running Tests

```bash
# Unit tests for a single service
docker compose exec auth-service pytest auth/tests/ -v

# Unit tests with coverage
docker compose exec auth-service pytest --cov=auth --cov-report=term-missing

# Integration tests (all services must be running)
pytest tests/integration/ -v --timeout=60

# Run all tests across all services
./scripts/run_all_tests.sh

# Watch mode during development
docker compose exec auth-service pytest-watch auth/tests/
```

### Test File Convention
```python
# tests/test_login.py

import pytest
from django.urls import reverse
from rest_framework.test import APIClient
from auth.models import AuthUser

@pytest.mark.django_db
class TestLogin:
    def setup_method(self):
        self.client = APIClient()
        self.user = AuthUser.objects.create_user(
            email="test@example.com",
            password="SecurePass123!"
        )

    def test_login_success(self):
        response = self.client.post(reverse("auth:login"), {
            "email": "test@example.com",
            "password": "SecurePass123!"
        })
        assert response.status_code == 200
        assert "access_token" in response.data
        assert "refresh_token" in response.data

    def test_login_wrong_password(self):
        response = self.client.post(reverse("auth:login"), {
            "email": "test@example.com",
            "password": "WrongPassword"
        })
        assert response.status_code == 401

    def test_login_unverified_email(self):
        self.user.is_verified = False
        self.user.save()
        response = self.client.post(reverse("auth:login"), {
            "email": "test@example.com",
            "password": "SecurePass123!"
        })
        assert response.status_code == 403
```

---

## 19. Implementation Roadmap

### Phase 1 — Foundation (Weeks 1–4)
| Week | Task | Owner | Status |
|---|---|---|---|
| 1 | Mono-repo setup, Docker Compose, base Dockerfiles | Both | ⬜ |
| 1 | Auth Service — register, login, JWT | Sayandip | ⬜ |
| 2 | User Service — profiles, addresses | Debabrata | ⬜ |
| 3 | NGINX basic routing config | Sayandip | ⬜ |
| 3 | GitHub Actions CI — lint + unit tests | Sayandip | ⬜ |
| 4 | Integration: Auth ↔ User Service communication | Both | ⬜ |

### Phase 2 — Core Commerce (Weeks 5–9)
| Week | Task | Owner | Status |
|---|---|---|---|
| 5 | Product Service — CRUD, categories, inventory | Sayandip | ⬜ |
| 6 | Cart & Wishlist Service | Sayandip | ⬜ |
| 6 | Order Service — create + state machine | Debabrata | ⬜ |
| 7 | Payment Service — Razorpay/mock integration | Debabrata | ⬜ |
| 7 | Kafka setup — topics + producers | Debabrata | ⬜ |
| 8 | Notification Service — Kafka consumers | Debabrata | ⬜ |
| 9 | Full checkout flow integration testing | Both | ⬜ |

### Phase 3 — Intelligence Layer (Weeks 10–14)
| Week | Task | Owner | Status |
|---|---|---|---|
| 10 | Elasticsearch — index products, search API | Sayandip | ⬜ |
| 11 | WebSocket Gateway — real-time order updates | Debabrata | ⬜ |
| 12 | AI Recommendation Service — Qdrant + embeddings | Sayandip | ⬜ |
| 13 | Analytics Service — ClickHouse + Kafka consumers | Debabrata | ⬜ |
| 14 | GraphQL Gateway | Both | ⬜ |

### Phase 4 — Production Hardening (Weeks 15–20)
| Week | Task | Owner | Status |
|---|---|---|---|
| 15 | Redis caching on Product Service | Sayandip | ⬜ |
| 16 | Load balancer + NGINX hardening (rate limits, SSL) | Sayandip | ⬜ |
| 17 | Cursor-based pagination across all services | Both | ⬜ |
| 18 | Full integration test suite | Both | ⬜ |
| 19 | Health checks + structured logging + correlation IDs | Both | ⬜ |
| 20 | README + API docs for each service | Both | ⬜ |

---

## 20. Team Ownership

| Service | Primary Owner | Reviewer |
|---|---|---|
| Auth Service | Sayandip | Debabrata |
| Product Service | Sayandip | Debabrata |
| Cart & Wishlist Service | Sayandip | Debabrata |
| Search Service | Sayandip | Debabrata |
| AI Recommendation Service | Sayandip | Debabrata |
| NGINX + Load Balancer | Sayandip | Debabrata |
| CI/CD Pipeline | Sayandip | Debabrata |
| User Service | Debabrata | Sayandip |
| Order Service | Debabrata | Sayandip |
| Payment Service | Debabrata | Sayandip |
| Notification Service | Debabrata | Sayandip |
| Analytics Service | Debabrata | Sayandip |
| WebSocket Gateway | Debabrata | Sayandip |
| Kafka Setup | Debabrata | Sayandip |
| GraphQL Gateway | Shared | Both |
| Docker Compose | Shared | Both |

### Infrastructure Ownership Rules
- Only **Sayandip** modifies `nginx/nginx.conf` (unless discussed)
- Only **Debabrata** modifies `kafka/topics.yml` (unless discussed)
- `docker-compose.yml` changes must be agreed upon by both and never fast-merged

---

## 21. Best Practices

### 12-Factor App Compliance
1. **Codebase** — One repo, tracked in Git
2. **Dependencies** — Explicit `requirements.txt` per service
3. **Config** — All config via environment variables, never hardcoded
4. **Backing Services** — DBs, Kafka, Redis treated as attached resources
5. **Build/Release/Run** — Docker separates build from run
6. **Processes** — Stateless services (state in Redis/DB, not in-memory)
7. **Port Binding** — Each service exports via its own port
8. **Concurrency** — Scale via multiple container instances
9. **Disposability** — Fast startup, graceful shutdown (SIGTERM handled)
10. **Dev/Prod Parity** — Docker Compose mirrors production
11. **Logs** — Written to stdout as JSON, collected by log aggregator
12. **Admin Processes** — Migrations run as one-off `docker exec` commands

### Security Checklist
- [ ] Passwords hashed with argon2 (never MD5/SHA1)
- [ ] JWTs expire in 15 minutes (not hours or days)
- [ ] Refresh token rotation on every use
- [ ] Webhook signatures verified with HMAC-SHA256
- [ ] Webhook endpoint NOT behind JWT auth, but behind IP whitelist
- [ ] All secrets in environment variables, never in code
- [ ] SQL injection impossible (ORM-based queries only)
- [ ] MongoDB injection impossible (mongoengine validation)
- [ ] CORS restricted to known origins
- [ ] Rate limiting at NGINX level (100 req/min per IP)
- [ ] HTTPS enforced (HTTP redirects to HTTPS)
- [ ] Payment gateway tokens stored AES-256 encrypted
- [ ] All financial amounts stored as DECIMAL, never FLOAT

### API Design Rules
- Use versioning: all endpoints under `/api/v1/`
- Use plural nouns: `/products/` not `/product/`
- Use HTTP verbs correctly: GET (read), POST (create), PATCH (partial update), PUT (full replace), DELETE
- Use correct status codes: 200 (OK), 201 (Created), 204 (No Content), 400 (Bad Request), 401 (Unauthenticated), 403 (Forbidden), 404 (Not Found), 422 (Validation Error), 429 (Rate Limited), 500 (Server Error)
- Cursor-based pagination on all list endpoints
- Consistent error response format across all services
- Never expose internal database IDs in URLs (use UUIDs)

### Database Rules
- Every service has its own database (no cross-service DB queries)
- All tables have UUID primary keys (never auto-increment integers)
- Financial amounts: `DECIMAL(12,2)` — never `FLOAT` or `DOUBLE`
- Passwords: never stored plain or in any reversible form
- Sensitive data: encrypted at rest where applicable
- Indexes: on all FK columns and frequently-queried fields
- Migrations: always backwards compatible (never drop column in same migration as add)

### Observability Standards
```python
# Every service logs in this JSON format:
{
  "timestamp": "2024-01-15T10:30:00.000Z",
  "level": "INFO",
  "service": "auth-service",
  "correlation_id": "req-uuid-from-header",
  "user_id": "user-uuid-or-null",
  "method": "POST",
  "path": "/api/v1/auth/login/",
  "status_code": 200,
  "duration_ms": 45,
  "message": "User login successful"
}
```

Every service must expose:
```
GET /health/
Response: {"status": "healthy", "database": "connected", "redis": "connected"}
Status: 200 (healthy) or 503 (unhealthy)
```

---

## 22. System Design Decisions

### Why Not a Monolith?
A monolith would be simpler to build but fails at scale. In an Amazon-like platform:
- Product catalog reads are 100x more frequent than writes — Product Service can scale independently
- Payment processing requires different reliability guarantees than cart management
- Different teams (in a real company) own different domains
- A bug in Notification Service should never take down Order Service

### Why Kafka Instead of Direct HTTP?
When an order is placed, 4 things need to happen: send confirmation email, update analytics, reserve inventory, push WebSocket update. With direct HTTP calls from Order Service:
- If Notification Service is down, the entire order creation fails
- Order Service now has to know about Notification, Analytics, Inventory — tight coupling
With Kafka, Order Service publishes one event and never thinks about consumers again. Consumers handle their own failures independently.

### Why Redis for Cart (Not PostgreSQL)?
Cart operations are extremely high-frequency. A user might update their cart 20 times in a shopping session. Redis HASH operations are O(1) and complete in under 1ms. Cart data is also naturally ephemeral — it should expire. Redis TTL handles this natively. If a user's cart is lost due to Redis restart, they can re-add items — a minor inconvenience vs. a significant performance overhead.

### Why Price Snapshots in Cart and Order?
If cart showed live prices, a user could add a product at ₹999, come back 3 hours later, and the seller raised the price to ₹1499. The user would be charged ₹1499 without consent. Price is snapshotted at add-time. The difference is shown as a warning. Order items also snapshot the price — so historical orders are always accurate even if the product price changes later.

### Why Cursor-Based Pagination?
Products are constantly being added and updated. With offset pagination (`LIMIT 20 OFFSET 40`), if 5 products are added to the beginning of the list between page 1 and page 2 requests, page 2 will repeat 5 items from page 1. Cursor-based pagination uses the last item's ID as a reference point — it's immune to insertions and performs better (`WHERE id > cursor` uses an index scan vs. offset which scans and discards).

### Why JSONB for Shipping Address in Orders?
When an order is placed, the shipping address is snapshotted into the order as JSONB. If the user later updates or deletes that address in User Service, the historical order still shows the correct delivery address. This is the right behaviour — you don't want historical orders to show a different address than what was used for delivery.

---

## 23. Known Limitations & Future Work

### Current Limitations
- No Kubernetes manifests (Docker Compose only)
- Single Kafka broker (not production-grade — needs 3 brokers for HA)
- Elasticsearch single node (no clustering)
- No distributed tracing (Jaeger/Zipkin not implemented)
- No API gateway product (Kong/AWS API Gateway) — custom NGINX routing
- No circuit breaker (Hystrix/Resilience4j pattern not implemented)
- Email sending is mocked (SendGrid integration stubbed)
- AI recommendations use a small local embedding model (not production-scale)

### Future Work
- [ ] Kubernetes manifests + Helm charts for production deployment
- [ ] Prometheus + Grafana for metrics and dashboards
- [ ] Jaeger for distributed tracing across services
- [ ] Circuit breaker pattern for inter-service HTTP calls
- [ ] Redis Sentinel or Redis Cluster for HA cache
- [ ] Kafka Schema Registry for event schema validation
- [ ] Seller portal service (manage products, view orders)
- [ ] Review & Rating Service
- [ ] Coupon & Discount Service
- [ ] Loyalty Points Service (listen to `order.delivered` events)
- [ ] A/B testing framework for recommendation algorithms
- [ ] Multi-region deployment with data locality

---

## Architecture Reference

📐 **Full System Design Diagrams:**  
[Open Miro Board →](https://miro.com/app/board/uXjVHMoCKt8=/)

The board contains:
1. High-Level System Design
2. Microservice Architecture with DB mapping
3. Service Communication Flow
4. Kafka Event Flow
5. CI/CD Pipeline
6. WebSocket Architecture
7. NGINX + Load Balancer Architecture
8. Elasticsearch Integration Flow
9. Team Ownership Map
10. Development Priority Roadmap
11. Folder Structure
12. LLD for all 6 core services (DB schemas + component flows)

---

<div align="center">

Built with ❤️ by **Sayandip** and **Debabrata**

*Production-grade microservices · Interview-ready system design · 5-month build*

</div>
