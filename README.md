# 🛒 ShopSphere — Go Modular Monolith E-Commerce Platform

<div align="center">

![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?style=for-the-badge&logo=go&logoColor=white)
![Gin](https://img.shields.io/badge/Gin-v1.9.1-008080?style=for-the-badge&logo=go&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?style=for-the-badge&logo=mongodb&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![Qdrant](https://img.shields.io/badge/Qdrant-Vector_DB-black?style=for-the-badge)
![Google GenAI SDK](https://img.shields.io/badge/Google_GenAI_SDK-Gemini-blue?style=for-the-badge)

**A fully production-grade, Amazon-like e-commerce backend built with a Go Modular Monolith architecture.**  
Designed for clean domain boundaries, scalability, and easy extraction into microservices later.

</div>

---

## 📋 Table of Contents

1. [Project Overview](#1-project-overview)
2. [System Architecture](#2-system-architecture)
3. [Tech Stack](#3-tech-stack)
4. [Modular Monolith Overview](#4-modular-monolith-overview)
5. [Database Design](#5-database-design)
6. [Module-by-Module Documentation](#6-module-by-module-documentation)
   - [Auth Module](#61-auth-module)
   - [User Module](#62-user-module)
   - [Product Module](#63-product-module)
   - [Cart & Wishlist Module](#64-cart--wishlist-module)
   - [Order Module](#65-order-service)
   - [Payment Module](#66-payment-service)
   - [Search Module](#67-search-service)
   - [AI Recommendation Module](#68-ai-recommendation-service)
   - [Notification Module](#69-notification-service)
   - [Analytics Module](#610-analytics-service)
   - [WebSocket Gateway](#611-websocket-gateway)
   - [GraphQL Gateway](#612-graphql-gateway)
7. [In-Memory Event Architecture](#7-in-memory-event-architecture)
8. [Folder Structure](#8-folder-structure)
9. [Getting Started](#9-getting-started)
10. [Docker Compose Reference](#10-docker-compose-reference)

---

## 1. Project Overview

ShopSphere is a **production-grade e-commerce platform** backend model. It is designed as a **Modular Monolith** in Go. This architecture organizes distinct business features (Auth, Users, Products, Carts, Orders, Payments, etc.) into isolated internal packages. This ensures:
- **Clean Boundaries** — modules interact through predefined interfaces and local event-dispatching, preventing spaghetti code.
- **Microservices Ready** — since domains are highly decoupled, any module can be extracted into an independent microservice later by simply replacing the interface with network RPCs (gRPC/HTTP).
- **A.I. Powered Recommendations** — uses the official Google GenAI Go SDK (`google.golang.org/genai`) to generate embeddings and Qdrant to lookup recommended products.
- **Polyglot Persistence** — different databases are used to store data depending on read/write requirements, keeping the database-per-module separation.

---

## 2. System Architecture

```
                               ┌────────────────────────────────┐
                               │     CLIENT (Web / Mobile)      │
                               └───────────────┬────────────────┘
                                               │ HTTP / WS
                               ┌───────────────▼────────────────┐
                               │      Gin Router / Gateway      │
                               └───────────────┬────────────────┘
                                               │
                       ┌───────────────────────┴───────────────────────┐
                       │                   Go Modular Monolith         │
                       │                                               │
                       │  ┌───────────┐  in-memory  ┌───────────────┐  │
                       │  │   Auth    ├────────────►│ Notification  │  │
                       │  └─────┬─────┘   events    └───────┬───────┘  │
                       │        │                           │          │
                       │  ┌─────▼─────┐             ┌───────▼───────┐  │
                       │  │  Product  │             │Recommendation│  │
                       │  └─────┬─────┘             └───────┬───────┘  │
                       │        │                           │          │
                       │  ┌─────▼─────┐             ┌───────▼───────┐  │
                       │  │   Order   │             │   Analytics   │  │
                       │  └─────┬─────┘             └───────┬───────┘  │
                       └────────┼───────────────────────────┼──────────┘
                                │                           │
             ┌──────────────────┼───────────────────────────┼──────────────────┐
             │ Datastore Layer  │                           │                  │
             │  ┌───────────┐   │   ┌───────────┐   ┌───────▼───┐  ┌─────────┐ │
             │  │PostgreSQL │◄──┼──►│   MySQL   │   │  Qdrant   │  │ClickHouse│ │
             │  └───────────┘   │   └───────────┘   └───────────┘  └─────────┘ │
             │                  │                                              │
             │  ┌───────────┐   │   ┌───────────┐   ┌───────────┐              │
             │  │  MongoDB  │◄──┴──►│   Redis   │   │Elastic     │              │
             │  └───────────┘       └───────────┘   └───────────┘              │
             └─────────────────────────────────────────────────────────────────┘
```

---

## 3. Tech Stack

### Languages & Frameworks
- **Go (Golang)**: `1.21+` core engine
- **Gin**: `v1.9.1` high-performance HTTP web framework
- **Gorilla WebSocket**: `v1.5.1` websocket connection upgrades
- **GraphQL Resolver**: Strawberry-like custom endpoint resolvers

### Databases & Infrastructure
- **PostgreSQL**: ACID transactions for Auth and Order modules
- **MySQL**: Relational data store for User Profiles and Payment transactions
- **MongoDB**: Flexible catalog model for Products and template storage for Notifications
- **Redis**: Caching, session store, cart details, and websocket pub-sub channel layer
- **Qdrant**: Vector Database storing Google Gemini generated product embeddings
- **Elasticsearch**: Full-text searching and aggregations
- **ClickHouse**: Columnar database storing high-throughput analytics metrics

### Go Libraries & SDKs
- **Google GenAI Go SDK (`google.golang.org/genai`)**: For generating embeddings via `text-embedding-004`
- **GORM**: ORM mapping PostgreSQL and MySQL relational databases
- **Go-Redis**: Redis client
- **Mongo-Driver**: MongoDB client
- **Qdrant Go Client**: Vector DB client
- **Golang-JWT**: Access token signature verification

---

## 4. Modular Monolith Overview

| Module | Datastore | Description |
|---|---|---|
| **Auth** | PostgreSQL + Redis | Registration, login, password resets, and JWT access tokens |
| **User** | MySQL | Profile parameters, avatar uploads, delivery addresses |
| **Product** | MongoDB + Redis | Custom catalog configurations, inventory listings, category trees |
| **Cart** | Redis | Temporary basket stores, item counts, wishlists |
| **Order** | PostgreSQL | Purchase creation, price tallies, delivery stages state machine |
| **Payment** | MySQL | Sandbox checkout adapters, invoice audit records |
| **Search** | Elasticsearch | Text similarity searches, custom index updates |
| **Recommendation** | Qdrant | Cosine similarity recommendations generated via Google GenAI SDK |
| **Notification** | MongoDB | In-memory message consumption, mailing lists, notification history logs |
| **Analytics** | ClickHouse | Local telemetry data collection, event summaries |
| **WebSocket** | Redis | Client connection pipelines, real-time purchase updates |
| **GraphQL** | — | GraphQL API wrapper exposing search, profiles, and order data |

---

## 5. Database Design

Each domain module maintains strict isolation of its schema and database connections. No module queries another module's database directly. Communication is handled in-memory using interface methods or event dispatch channels.

---

## 6. Module-by-Module Documentation

### 6.1 Auth Module
- **Endpoints mapping:** `/api/v1/auth`
- **Endpoints:**
  - `POST /register` — Register a new account
  - `POST /login` — Authenticate credentials, issues JWT pairs
  - `POST /refresh` — Refresh access token using rotation
  - `POST /logout` — Revoke and blacklist refresh token in Redis
  - `POST /verify-email` — Complete email validation
  - `GET /me` — Returns logged-in basic user parameters

### 6.2 User Module
- **Endpoints mapping:** `/api/v1/users`
- **Endpoints:**
  - `GET /me` — Retrieve customer profile
  - `PATCH /me` — Update profile elements
  - `GET /me/addresses` — Fetch shipping addresses (max 10 addresses)
  - `POST /me/addresses` — Store a new shipping address
  - `DELETE /me/addresses/:id` — Delete address profile

### 6.3 Product Module
- **Endpoints mapping:** `/api/v1/products`, `/api/v1/categories`
- **Endpoints:**
  - `GET /` — Paginated and filterable catalogs
  - `POST /` — Add inventory items
  - `GET /:id` — Detailed product view with cache-aside lookup
  - `PATCH /:id` — Update listing details
  - `DELETE /:id` — Archive listing

### 6.4 Cart & Wishlist Module
- **Endpoints mapping:** `/api/v1/cart`, `/api/v1/wishlist`
- **Endpoints:**
  - `GET /cart` — View current basket items
  - `POST /cart/items` — Add items to cart
  - `DELETE /cart/items/:id` — Drop cart item
  - `GET /wishlist` — View wishlist items

### 6.5 Order Module
- **Endpoints mapping:** `/api/v1/orders`
- **Endpoints:**
  - `GET /` — List order history
  - `POST /` — Initiate purchase (PENDING -> CONFIRMED)
  - `GET /:id` — View status of order
  - `POST /:id/cancel` — Cancel pending order

### 6.6 Payment Module
- **Endpoints mapping:** `/api/v1/payments`
- **Endpoints:**
  - `POST /checkout` — Initiate mock payment transaction
  - `POST /webhook` — Receive invoice status update hooks

### 6.7 Search Module
- **Endpoints mapping:** `/api/v1/search`
- **Endpoints:**
  - `GET /` — Elastic full-text queries

### 6.8 AI Recommendation Service
- **Endpoints mapping:** `/api/v1/recommendations`
- **Endpoints:**
  - `GET /product/:product_id` — Fetch similar items using cosine-similarity on vector embeddings
  - `GET /user/:user_id` — Custom tailored recommendations
  - `POST /embed` — Generates and uploads embedding values to Qdrant Vector DB
- **Integration with Google GenAI SDK:**
  Generates embeddings on product updates (`name + description + brand + tags`) using:
  ```go
  client.Models.EmbedContent(ctx, "text-embedding-004", contents, nil)
  ```

### 6.9 Notification Module
- Subscribes to in-memory events like `user.registered` or `order.created`.
- Dispatches emails/SMS templates logged into MongoDB.

### 6.10 Analytics Module
- Listens to system-wide metrics and aggregates ClickHouse telemetry.

### 6.11 WebSocket Gateway
- Upgrades incoming routes (`/api/v1/ws/orders/:order_id`) to listen to live state modifications.

### 6.12 GraphQL Gateway
- Proxies HTTP calls for unified client dashboards via `/api/v1/graphql`.

---

## 7. In-Memory Event Architecture

To support clean decoupling, we use an in-memory Event Dispatcher (`internal/events`). Modules can subscribe to topic names (e.g. `user.registered`) and execute non-blocking operations:

```go
// Register subscription in notification module
events.Subscribe("user.registered", func(payload interface{}) {
    // Send email using MongoDB templates
})

// Dispatch event in auth module
events.Publish("user.registered", userPayload)
```

---

## 8. Folder Structure

```
shopsphere/
├── cmd/
│   └── server/
│       └── main.go             # Monolith entry point
├── config/
│   └── config.go               # Configurations environment variables loader
├── internal/
│   ├── app/                    # Monolith initialization logic
│   │   └── app.go
│   ├── events/                 # In-memory Event dispatcher
│   │   └── dispatcher.go
│   # -- Domain Modules --
│   ├── auth/
│   │   ├── handler.go          
│   │   └── model.go
│   ├── user/
│   │   ├── handler.go          
│   │   └── model.go
│   ├── product/
│   │   ├── handler.go          
│   │   └── model.go
│   ├── cart/
│   │   ├── handler.go          
│   │   └── model.go
│   ├── order/
│   │   ├── handler.go          
│   │   └── model.go
│   ├── payment/
│   │   ├── handler.go          
│   │   └── model.go
│   ├── search/
│   │   ├── handler.go          
│   │   └── model.go
│   ├── recommendation/
│   │   ├── embeddings.go       # Google GenAI Go SDK client integrations
│   │   ├── handler.go          
│   │   └── model.go
│   ├── notification/
│   │   └── handler.go          
│   ├── analytics/
│   │   └── handler.go          
│   ├── websocket/
│   │   └── handler.go          
│   └── graphql/
│       └── handler.go          
├── pkg/
│   └── logger/
│       └── logger.go
├── Dockerfile                  # Go Monolith Dockerfile
├── docker-compose.yml          # App + Database setup
├── go.mod                      # Go Module definitions
└── README.md                   # Project documentation
```

---

## 9. Getting Started

### 1. Build and Start the Datastore Containers
```bash
docker compose up -d postgres mysql mongodb redis qdrant elasticsearch clickhouse
```

### 2. Configure Environment variables
Create a `.env` file at the root:
```env
PORT=8080
NODE_ENV=development
APP_NAME=shopsphere
GOOGLE_API_KEY=your-gemini-api-key
```

### 3. Build & Run the Go Monolith locally
```bash
go build -o server cmd/server/main.go
./server
```
