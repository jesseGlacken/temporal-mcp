# Dokploy Setup (Ordinatude)

## Prerequisites

- Temporal stack running in Dokploy.
- Name of its Docker network: `docker network ls | grep temporal`
- Authentik service account in a group with access to the target namespace(s).

## 1. Get a Temporal token

In the Authentik provider used by Temporal, raise the access-token validity. Then:

```sh
curl -s https://<authentik>/application/o/token/ \
  -d grant_type=client_credentials \
  -d client_id=<client-id> \
  -d username=<service-account> \
  -d password=<app-password> \
  -d scope='openid profile email'
```

Copy `access_token`.

## 2. Create the service

1. Dokploy → New service → Compose → this repo, compose path `docker-compose.yml`.
2. Environment tab: paste `.env.ordinatude.example`, then set:
   - `TEMPORAL_DOCKER_NETWORK`: the Temporal stack's network
   - `TEMPORAL_HOST`: Temporal gRPC endpoint on that network (default `temporal:7233`)
   - `TEMPORAL_API_KEY`: token from step 1
   - `TEMPORAL_NAMESPACE` / `TEMPORAL_ALLOWED_NAMESPACES`
3. Domains tab: service `temporal-mcp`, port `8000`, path `/`, HTTPS on.
4. Deploy.

## 3. Connect

- Streamable HTTP: `https://<domain>/mcp`
- SSE: `https://<domain>/sse`

## Optional: basic auth

Uncomment the `labels` in `docker-compose.yml` and set `MCP_BASIC_AUTH_USERS`:

```sh
htpasswd -nbB <user> '<pass>' | sed -e 's/\$/\$\$/g'
```

Or skip step 2.3 to keep the endpoint internal only.

## Token renewal

When the token expires: repeat step 1, update `TEMPORAL_API_KEY`, redeploy.
