# Appendix B: API Key Registration

[← Back to Table of Contents](00_COVER_AND_TOC.md)

---

This appendix provides step-by-step instructions for registering API keys required by the platform.

## Required API Keys

### USDA NASS QuickStats API

**Used for:** Crop progress, acreage, production, stocks

**Registration:**

1. Go to https://quickstats.nass.usda.gov/api
2. Click **Request API Key**
3. Fill in the form:
   - Name: Your name
   - Email: Your work email
   - Organization: Round Lakes Commodities
   - Intended Use: "Automated commodity market analysis"
4. Click **Submit**
5. Check your email for the API key (usually instant)
6. Add to `.env`:
   ```
   NASS_API_KEY=your-key-here
   ```

**Rate Limits:** 10,000 requests/day

---

### US Energy Information Administration (EIA)

**Used for:** Ethanol production, petroleum data, energy prices

**Registration:**

1. Go to https://www.eia.gov/opendata/register.php
2. Fill in registration form:
   - Email address
   - Name
   - Organization
   - Reason: "Commodity market analysis and research"
3. Click **Register**
4. Check email for API key
5. Add to `.env`:
   ```
   EIA_API_KEY=your-key-here
   ```

**Rate Limits:** No published limit, but be respectful (< 1,000 requests/hour)

---

### US Census Bureau

**Used for:** Import/export trade data by HS code

**Registration:**

1. Go to https://api.census.gov/data/key_signup.html
2. Fill in form:
   - Email address
   - Organization: Round Lakes Commodities
3. Agree to Terms of Service
4. Click **Submit**
5. Check email for API key (usually within minutes)
6. Add to `.env`:
   ```
   CENSUS_API_KEY=your-key-here
   ```

**Rate Limits:** 500 queries per IP per day without key, unlimited with key

---

## Optional API Keys

### Tavily (Web Search)

**Used for:** LLM web research, current news

**Registration:**

1. Go to https://tavily.com
2. Click **Get Started** or **Sign Up**
3. Create account (Google/GitHub or email)
4. Navigate to API section in dashboard
5. Copy API key
6. Add to `.env`:
   ```
   TAVILY_API_KEY=your-key-here
   ```

**Free Tier:** 1,000 searches/month

---

### Notion

**Used for:** Knowledge base, long-term memory

**Registration:**

1. Go to https://www.notion.so/my-integrations
2. Click **New integration**
3. Name: "RLC Data Platform"
4. Select workspace
5. Click **Submit**
6. Copy the Internal Integration Token
7. Add to `.env`:
   ```
   NOTION_API_KEY=your-token-here
   ```

**Additional Step:** Share relevant Notion pages with the integration

---

### OpenWeather API

**Used for:** Weather forecasts for crop regions

**Registration:**

1. Go to https://openweathermap.org/api
2. Click **Sign Up**
3. Create account
4. Navigate to **API Keys** tab
5. Copy your key (or generate a new one)
6. Add to `.env`:
   ```
   OPENWEATHER_API_KEY=your-key-here
   ```

**Free Tier:** 1,000 calls/day, current weather and 5-day forecast

---

### Dropbox

**Used for:** Report distribution, file sharing

**Registration:**

1. Go to https://www.dropbox.com/developers/apps
2. Click **Create App**
3. Choose: Scoped access, Full Dropbox
4. Name: "RLC Reports"
5. Click **Create**
6. Under Permissions, enable:
   - files.content.write
   - files.content.read
7. Generate access token
8. Add to `.env`:
   ```
   DROPBOX_ACCESS_TOKEN=your-token-here
   ```

---

## API Key Best Practices

| Practice | Reason |
|----------|--------|
| Use individual keys per user | Track usage, limit exposure |
| Rotate keys annually | Security hygiene |
| Monitor usage dashboards | Catch abuse early |
| Don't commit to Git | Prevent exposure |
| Use environment variables | Separation of config from code |

---

## Troubleshooting API Issues

| Error | Cause | Solution |
|-------|-------|----------|
| 401 Unauthorized | Invalid or expired key | Regenerate key, update `.env` |
| 403 Forbidden | Key not authorized for endpoint | Check API documentation for permissions |
| 429 Too Many Requests | Rate limit exceeded | Wait and retry, or reduce request frequency |
| Connection timeout | Network or API outage | Check API status page, retry later |

---

## Quick Test Commands

After adding keys, verify they work:

```bash
# Test NASS API
curl "https://quickstats.nass.usda.gov/api/api_GET/?key=YOUR_KEY&source_desc=SURVEY&commodity_desc=CORN&year=2024&format=JSON" | head

# Test EIA API
curl "https://api.eia.gov/v2/petroleum/sum/sndw/data?api_key=YOUR_KEY&frequency=weekly&data[0]=value" | head

# Test Census API
curl "https://api.census.gov/data/timeseries/intltrade/exports/hs?key=YOUR_KEY&get=CTY_CODE,ALL_VAL_MO&time=2024-01&COMM_LVL=HS2" | head
```

---

[← Back to Table of Contents](00_COVER_AND_TOC.md) | [Next: Database Quick Reference →](APPENDIX_C_DATABASE_REFERENCE.md)
