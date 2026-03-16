# Anomaly Report – NUAAV Data Ingestion

> **Scope**: All source files in `input_data/` examined prior to Step 1.
> Anomalies are documented here so transformation decisions in the staging
> layer are traceable and auditable.

---

## 1. Delivery Structure Finding – "Client B" vs Client C

| Item | Detail |
|------|--------|
| Folder name | `input_data/Client B/` |
| Internal file labels | `clientC_customers.csv`, `clientC_orders.csv`, … |
| JSON client field | `"client": "ClientC"` |
| ID prefixes | `C-CUST-`, `C-ORD-`, `C-SKU-`, `C-TXN-` |
| **Conclusion** | **No Client B data exists.** The folder is a delivery naming error. All data in it belongs to Client C. A placeholder row for Client B is seeded in `dim_client` with `is_active = FALSE`. |

---

## 2. Client A – Anomalies

### 2.1 `Customer.csv`

| customer_id | Anomaly | Handling |
|-------------|---------|----------|
| CUST-A-0003 | `loyalty_tier` is blank | Set to `UNKNOWN` in staging |
| CUST-A-0004 | `first_name`, `last_name`, `email` all missing | Flagged; retained with NULLs |
| CUST-A-0005 | Email `cevans@example` has no TLD | `is_email_valid = FALSE` |
| CUST-A-0008 | `loyalty_tier` blank | Set to `UNKNOWN` |
| CUST-A-0011 | `loyalty_tier` blank | Set to `UNKNOWN` |
| CUST-A-0015 | `email` missing | Retained; `is_email_valid = FALSE` |
| CUST-A-0017 | `loyalty_tier` blank | Set to `UNKNOWN` |

### 2.2 `Orders.csv`

| order_id | Anomaly | Handling |
|----------|---------|----------|
| ORD-5004 | `customer_id = CUST-A-9999` (not in customers) | `HAS_INVALID_REF = TRUE`; `customer_key = NULL` in fact |
| ORD-5005 | `order_date` missing | `HAS_MISSING_DATE = TRUE`; `date_key = NULL` |
| ORD-5008 | `order_date` missing | Same as above |

> **Note**: The `channel` field in several rows contains trailing annotation
> text (e.g., `Web      <-- invalid customer`). This is a source artefact.
> Staging strips everything after the first two or more spaces using
> `REGEXP_REPLACE(channel, '\\s{2,}.*$', '')`.

### 2.3 `Products.csv`

| sku | Anomaly | Handling |
|-----|---------|----------|
| SKU-A-011 | `unit_price = -9.99` | `is_price_valid = FALSE`; excluded from revenue metrics |

### 2.4 XML Transactions (`ClientA_Transactions_1.xml` … `_7.xml`, `_4.txt`)

| transaction_id | Anomaly | Handling |
|----------------|---------|----------|
| TXN-1001 | Duplicate row (appears twice across files) | Keep first occurrence; `is_duplicate = TRUE` on second |
| TXN-1001 | Second copy uses `<LastLastName>` instead of `<LastName>` | XMLGET path patched in staging; unknown tag yields NULL |
| TXN-1001 | Item with `quantity = -1` | `HAS_NEGATIVE_QTY = TRUE` |
| TXN-1005 | `payment.amount = -89.99` | `HAS_NEGATIVE_AMT = TRUE` |
| TXN-1006 | `sku` element is empty | `HAS_MISSING_SKU = TRUE`; `product_key = NULL` |
| TXN-1016 | `email` element is empty; extra `<Notes>` node | Email NULL; extra nodes ignored by XMLGET |
| TXN-1017 | `sku` empty; negative payment amount | Both flags set |

---

## 3. Client C – Anomalies

### 3.1 `Customer.CSV`

| customer_id | Anomaly | Handling |
|-------------|---------|----------|
| C-CUST-5006 | `customer_name` NULL; email `noemail@` (no domain) | Name NULL; `is_email_valid = FALSE` |
| C-CUST-5010 | Name `Unknown User`; segment `UNKNOWN` | Loaded as-is; `loyalty_tier = UNKNOWN` |
| C-CUST-5013 | Email missing | `is_email_valid = FALSE` |

### 3.2 `Order.csv`

| order_id | Anomaly | Handling |
|----------|---------|----------|
| C-ORD-9004 | `order_date` missing | `HAS_MISSING_DATE = TRUE` |
| C-ORD-9008 | `customer_id = C-CUST-9999` (not in customers) | `HAS_INVALID_REF = TRUE` |
| C-ORD-9019 | `order_date` missing | `HAS_MISSING_DATE = TRUE` |
| C-ORD-9001 (line 22) | Exact duplicate of line 4 | `is_duplicate = TRUE` on second occurrence |

### 3.3 `Product.csv`

| sku | Anomaly | Handling |
|-----|---------|----------|
| C-SKU-003 (duplicate) | Exact duplicate row | Deduplicated; second row dropped |
| C-SKU-011 | `unit_price = -59.99` | `is_price_valid = FALSE` |
| C-SKU-999 | `product_name = Unknown Product`; `unit_price = 0.00`; `is_active = false` | Loaded; `is_price_valid = FALSE` for zero price |

### 3.4 `Payments.csv`

| payment_id | Anomaly | Handling |
|------------|---------|----------|
| PAY-C-0005 | `amount = -10.00`, `status = REFUNDED` | Negative amount on REFUNDED is valid; `HAS_NEGATIVE_AMT = FALSE` |
| PAY-C-0007 | `amount = 0.00`, `status = SETTLED` | Suspicious; flagged with `HAS_NEGATIVE_AMT = FALSE` but noted in `ANOMALY_NOTES` |
| PAY-C-0008 | `amount = 0.00`, `status = SETTLED` (linked to orphan order) | Same as above |
| PAY-C-0017 | `amount = -49.99`, `status = REFUNDED` | Valid refund; not flagged |
| PAY-C-0001 (duplicate) | Exact duplicate of first row | `is_duplicate = TRUE` on second |

### 3.5 `transactions.json`

| transaction_id | Anomaly | Handling |
|----------------|---------|----------|
| C-TXN-3001 (first) | Valid record with 1 item | Kept |
| C-TXN-3001 (second) | Duplicate id; `items` array is empty | `is_duplicate = TRUE`; row excluded from line-item expansion |

---

## 4. Cross-Cutting Rules Applied in Staging

| Rule | Implementation |
|------|---------------|
| Deduplicate on natural key | `ROW_NUMBER() OVER (PARTITION BY <natural_key> ORDER BY ...)` – keep row 1 |
| Strip inline comments | `REGEXP_REPLACE(col, '\\s{2,}.*$', '')` on string columns |
| Email validation | `col REGEXP '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+'` |
| Negative price guard | `unit_price <= 0 → is_price_valid = FALSE` |
| Negative quantity guard | `quantity < 0 → has_negative_qty = TRUE` |
| Missing date guard | `TRY_TO_DATE(order_date) IS NULL → has_missing_date = TRUE` |
| Loyalty tier mapping | Client C `VIP → PLATINUM`, `REGULAR → SILVER`, `NEW → BRONZE`, `UNKNOWN → UNKNOWN` |
| Unknown loyalty tier | Any NULL or unrecognised value → `UNKNOWN` |

---

## 5. Open Questions / Decisions Needed

1. **Client B**: Confirm whether Client B data will arrive in future batches and what its schema looks like.
2. **Negative refunds**: Treat refunded amounts (negative, status=REFUNDED) as valid signed amounts or as absolute values?  Current approach: keep as-is so revenue sums correctly reflect refunds.
3. **Zero-amount settled payments**: PAY-C-0007 and PAY-C-0008 have amount 0 but status SETTLED. Investigate with Client C whether these are data entry errors.
4. **Orphan customer CUST-A-9999 / C-CUST-9999**: Both appear to be test or bad-data IDs.  Current approach: keep the fact row with `customer_key = NULL` and set `HAS_INVALID_REF = TRUE`.
