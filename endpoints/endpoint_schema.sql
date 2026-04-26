SET ROLE proxy_whitelist_owner;

/** openapi
openapi: 3.1.0
info:
  title: Proxy Whitelist
  description: >-
    API for checking image URL whitelist/blacklist status on the Hive blockchain.
    URLs that appear in Hive posts are whitelisted for proxying by the imagehoster.
  license:
    name: MIT License
    url: https://opensource.org/license/mit
  version: 1.0.0
externalDocs:
  description: proxy-whitelist gitlab repository
  url: https://gitlab.syncad.com/hive/proxy-whitelist
tags:
  - name: Whitelist
    description: URL whitelist and blacklist checks
  - name: Accounts
    description: Account-URL relationship queries
servers:
  - url: /proxy-whitelist-api
 */

DO $__$
    BEGIN
        CREATE SCHEMA IF NOT EXISTS proxy_whitelist_endpoints AUTHORIZATION proxy_whitelist_owner;

        CREATE OR REPLACE FUNCTION proxy_whitelist_endpoints.root() RETURNS json AS $_$
        DECLARE
        -- openapi-spec
-- openapi-generated-code-begin
  openapi json = $$
{
  "openapi": "3.1.0",
  "info": {
    "title": "Proxy Whitelist",
    "description": "API for checking image URL whitelist/blacklist status on the Hive blockchain.",
    "license": {
      "name": "MIT License",
      "url": "https://opensource.org/license/mit"
    },
    "version": "1.0.0"
  },
  "externalDocs": {
    "description": "proxy-whitelist gitlab repository",
    "url": "https://gitlab.syncad.com/hive/proxy-whitelist"
  },
  "tags": [
    {
      "name": "Whitelist",
      "description": "URL whitelist and blacklist checks"
    },
    {
      "name": "Accounts",
      "description": "Account-URL relationship queries"
    }
  ],
  "servers": [
    {
      "url": "/proxy-whitelist-api"
    }
  ],
  "paths": {
    "/whitelist/check": {
      "post": {
        "tags": ["Whitelist"],
        "summary": "Check whitelist/blacklist status of a URL",
        "description": "Returns 'blacklisted' if the URL is on the permanent URL blacklist, 'whitelisted' if it has been referenced in a Hive post, or 'unknown' otherwise. If blacklisted_accounts is supplied, a URL whose only authors are all in that list is reported as 'unknown' for this caller (lets each caller apply its own per-account blacklist without storing operator-specific state in the database).",
        "operationId": "proxy_whitelist_endpoints.check_url",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["url"],
                "properties": {
                  "url": {
                    "type": "string",
                    "description": "The URL to check"
                  },
                  "blacklisted_accounts": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Optional list of Hive account names to treat as blacklisted for this call. URLs whose authors are all in this list are returned as 'unknown'."
                  }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "One of 'blacklisted', 'whitelisted', or 'unknown'.",
            "content": {
              "application/json": {
                "schema": {
                  "type": "string",
                  "enum": ["blacklisted", "whitelisted", "unknown"]
                }
              }
            }
          }
        }
      }
    },
    "/whitelist/url-blacklisted": {
      "post": {
        "tags": ["Whitelist"],
        "summary": "Check if a URL is permanently blacklisted",
        "operationId": "proxy_whitelist_endpoints.is_url_blacklisted",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["url"],
                "properties": {
                  "url": {
                    "type": "string",
                    "description": "The URL to check"
                  }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Boolean result",
            "content": {
              "application/json": {
                "schema": { "type": "boolean" }
              }
            }
          }
        }
      }
    },
    "/whitelist/account-urls": {
      "post": {
        "tags": ["Accounts"],
        "summary": "Get all URLs posted by an account",
        "operationId": "proxy_whitelist_endpoints.account_urls",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["account"],
                "properties": {
                  "account": {
                    "type": "string",
                    "description": "Hive account name"
                  }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Array of URL strings",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "url": { "type": "string" }
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/whitelist/url-accounts": {
      "post": {
        "tags": ["Accounts"],
        "summary": "Get all accounts that posted a URL",
        "operationId": "proxy_whitelist_endpoints.url_accounts",
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["url"],
                "properties": {
                  "url": {
                    "type": "string",
                    "description": "The URL to look up"
                  }
                }
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Array of account names",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "account": { "type": "string" }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
$$;
-- openapi-generated-code-end
BEGIN
  RETURN openapi;
END
$_$ LANGUAGE plpgsql;

    END
$__$;

GRANT USAGE ON SCHEMA proxy_whitelist_endpoints TO proxy_whitelist_user;

RESET ROLE;
