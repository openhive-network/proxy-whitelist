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
  - name: Other
    description: General API information
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
    },
    {
      "name": "Other",
      "description": "General API information"
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
        "summary": "Check if a URL is whitelisted",
        "description": "Returns true if the URL exists in the whitelist (has been referenced in a Hive post).",
        "operationId": "proxy_whitelist_endpoints.is_whitelisted",
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
    },
    "/sync-status": {
      "get": {
        "tags": ["Other"],
        "summary": "Get proxy whitelist's sync status",
        "description": "Get the last block processed by the proxy whitelist app as an object containing both the block number and its timestamp (UTC). This is the uniform HAF-app sync/health endpoint: the timestamp lets a consumer compute staleness with a single call (age = now() - last_block_time) without needing a separate head-block reference.",
        "operationId": "proxy_whitelist_endpoints.sync_status",
        "responses": {
          "200": {
            "description": "Last block processed and its timestamp. last_block_time is null if no block has been processed yet.",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "last_block_num": {
                      "type": "integer",
                      "description": "highest block number processed by the app"
                    },
                    "last_block_time": {
                      "type": "string",
                      "format": "date-time",
                      "description": "UTC timestamp of that block"
                    }
                  }
                },
                "example": {
                  "last_block_num": 5000000,
                  "last_block_time": "2016-09-15T19:47:21"
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
