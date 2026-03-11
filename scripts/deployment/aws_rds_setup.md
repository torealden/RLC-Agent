# AWS RDS PostgreSQL Setup Guide

## Step 1: Create AWS Account

1. Go to https://aws.amazon.com and click "Create an AWS Account"
2. Enter email, password, account name
3. Add payment method (credit card required, but RDS free tier gives 12 months of db.t3.micro)
4. Choose "Basic (Free)" support plan

## Step 2: Create RDS PostgreSQL Instance

1. Sign in to AWS Console → search "RDS" → click "Create database"

2. Use these settings:

| Setting | Value |
|---------|-------|
| Creation method | Standard create |
| Engine | PostgreSQL |
| Engine version | 16.x (latest) |
| Templates | **Free tier** |
| DB instance identifier | `rlc-commodities` |
| Master username | `postgres` |
| Master password | *(choose a strong password, you'll need it later)* |
| DB instance class | db.t3.micro (Free tier) |
| Storage type | gp3 |
| Allocated storage | 20 GB |
| Storage autoscaling | Enable, max 100 GB |
| **Connectivity** | |
| VPC | Default VPC |
| Public access | **Yes** *(required for your local machine and Felipe to connect)* |
| VPC security group | Create new → name it `rlc-db-access` |
| Database port | 5432 |
| **Additional configuration** | |
| Initial database name | `rlc_commodities` |
| Backup retention | 7 days |
| Monitoring | Enable (free basic) |

3. Click **Create database** — takes 5-10 minutes to provision

## Step 3: Configure Security Group

After the instance is created:

1. Click on the DB instance → **Connectivity & security** tab
2. Click the security group link (e.g., `sg-xxxxx (rlc-db-access)`)
3. Click **Inbound rules** → **Edit inbound rules**
4. Add rules:

| Type | Port | Source | Description |
|------|------|--------|-------------|
| PostgreSQL | 5432 | My IP | Your home IP |
| PostgreSQL | 5432 | *(Felipe's IP)* | Felipe's access |
| PostgreSQL | 5432 | *(future: EC2 instance IP)* | LLM server |

5. Click **Save rules**

**To find your IP:** Google "what is my IP" — it will show your public IP.

## Step 4: Get Your Endpoint

1. In RDS Console → click your instance → **Connectivity & security**
2. Copy the **Endpoint** — it looks like:
   ```
   rlc-commodities.xxxxxxxxxxxx.us-east-1.rds.amazonaws.com
   ```
3. **Port** is 5432

## Step 5: Test Connection

Open a terminal and test:
```bash
psql -h rlc-commodities.xxxxxxxxxxxx.us-east-1.rds.amazonaws.com -U postgres -d rlc_commodities
```

Or run:
```bash
python scripts/deployment/migrate_to_rds.py --test-only --host rlc-commodities.xxxx.us-east-1.rds.amazonaws.com --password YOUR_PASSWORD
```

## Step 6: Migrate Data

```bash
python scripts/deployment/migrate_to_rds.py \
  --host rlc-commodities.xxxx.us-east-1.rds.amazonaws.com \
  --password YOUR_RDS_PASSWORD
```

This will:
1. Dump your local database (schemas + data)
2. Restore to RDS
3. Verify table counts match
4. Update your .env with the new connection

## Step 7: Update VBA Workbooks

After migration, update the DB_SERVER constant in each VBA module:
- Change `"localhost"` to your RDS endpoint
- Change the password to your RDS password

## Cost Estimate

| Item | Monthly Cost |
|------|-------------|
| db.t3.micro (Free tier year 1) | $0 |
| db.t3.micro (after free tier) | ~$15 |
| Storage (20 GB gp3) | ~$2.30 |
| Data transfer (modest) | ~$1-3 |
| **Total (year 1)** | **~$3-5/mo** |
| **Total (after year 1)** | **~$18-20/mo** |
