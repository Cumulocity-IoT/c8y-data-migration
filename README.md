# 🚀 c8y-data-migration

**c8y-data-migration** is a Cumulocity microservice that migrates signal data (Measurements, Events, and Alarms) from a source tenant to a target tenant.

---

## 📋 Pre-requisites
- Ensure **Lombok** (version **1.18.20**) is installed and enabled in your IDE.
- Devices must exist in the **Target tenant** with the same **External ID** as in the Source tenant (this service uses External IDs for mapping).

---

## 🛠️ Setup and Run

### Step 1: Create Microservice
Send a **POST** request to create a microservice:

**URL:** `{{url}}/application/applications`

**Body:**
```json
{
  "requiredRoles": [
    "ROLE_ALARM_ADMIN",
    "ROLE_ALARM_READ",
    "ROLE_DEVICE_CONTROL_ADMIN",
    "ROLE_DEVICE_CONTROL_READ",
    "ROLE_EVENT_ADMIN",
    "ROLE_EVENT_READ",
    "ROLE_IDENTITY_ADMIN",
    "ROLE_IDENTITY_READ",
    "ROLE_INVENTORY_CREATE",
    "ROLE_INVENTORY_ADMIN",
    "ROLE_INVENTORY_READ",
    "ROLE_MEASUREMENT_ADMIN",
    "ROLE_MEASUREMENT_READ",
    "ROLE_OPTION_MANAGEMENT_READ",
    "ROLE_OPTION_MANAGEMENT_ADMIN"
  ],
  "contextPath": "data-migration",
  "type": "MICROSERVICE",
  "name": "data-migration",
  "key": "data-migration-key"
}
```

### Step 2: Subscribe Microservice
- Navigate to **Ecosystem > Microservices > Data-migration > Properties** in the **Administration** application.
- Subscribe the microservice.

### Step 3: Get Bootstrap Credentials

**Method:** `GET`

**URL:** `{{url}}/application/applications/<application-ID>/bootstrapUser`

- Use the returned credentials for microservice configuration.

### Step 4: Configure Environment Properties
- Copy content from `application-env-TEMPLATE.properties` into a new file named `application.properties`.
- Fill in the required credentials obtained in Step 3.

### Step 5: Run Locally
- Start the microservice locally using the filled `application.properties`.

---

## 🔥 Migration Related APIs

### 1. Collect Available Signal Data
Use this to check the available signals for one or more devices.

**Method:** `POST`

**URL:** `http://localhost:8070/signalmetrics/collect`

**Body:**
```json
{
  "sourcePlatformLoginString": "<tenant-id>/<username>:<password>",
  "sourcePlatformHost": "<tenant-url>",
  "sourceDevicesQuery": "name eq 'tedge-test'", // Adjust the query as required. Can fetch single or multiple devices based on query. This is used for filtering required devices
  "dateFrom": "2024-07-04T05:03:23.157Z", // Adjust accordingly
  "dateTo": "2025-07-04T05:03:23.157Z" // Adjust accordingly
}
```

### 2. Run a Migration Job
To migrate the data:

**Method:** `POST`

**URL:** `http://localhost:8070/migrationjob`

**Body:**
```json
{
  "jobName": "<job-name>",
  "signalType": "EVENT",
  "sourcePlatformLoginString": "<source-tenant-id>/<username>:<password>",
  "sourcePlatformHost": "<source-tenant-url>",
  "targetPlatformLoginString": "<target-tenant-id>/<username>:<password>",
  "targetPlatformHost": "<target-tenant-url>",
  "sourceDevicesQuery": "name eq 'tedge-test'", // Adjust the query as required. Can fetch single or multiple devices based on query. This is used for filtering required devices
  "dateFrom": "2024-07-04T05:03:23.157Z", // Adjust accordingly
  "dateTo": "2025-07-04T05:03:23.157Z" // Adjust accordingly
}
```

### 3. Retry a Failed Job
To retry a failed migration job:

**Method:** `POST`

**URL:** `http://localhost:8070/migrationjob/retry/{jobId}`

---

## 📈 Features and Advantages

- ✅ Migrate data for **single or multiple devices** simultaneously.
- ✅ **Retry** failed jobs — continues from the point where it failed.
- ✅ Easily **configure** Source and Target tenants.
- ✅ Choose the type of signal to migrate (**MEASUREMENT, EVENT, ALARM**).
- ✅ **Specify date range** for the data to be migrated.
- ✅ **Specify the query** to filter devices to be migrated.

---

