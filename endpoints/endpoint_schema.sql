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
      "name": "Profiles",
      "description": "Account avatar and cover image URLs"
    },
    {
      "name": "Streams",
      "description": "Block-ordered pages of the app's tables for replicas. Rows carry the block that introduced (urls, url authors) or last changed (profiles) them; rows older than the streams carry the sentinel block of /sync-status. A page covers [from_block, to_block] (to_block capped at the last processed block, returned in the Stream-To-Block header; the source identity is in Stream-Sync-Uuid) ordered by (block, natural key); its size is the limit query parameter, at most 100000. A page shorter than limit completes the range; otherwise request the next one with from_block set to the last row's block and after_* set to its key."
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
    },
    "/stream/accounts": {
      "get": {
        "tags": [
          "Streams"
        ],
        "summary": "Page of accounts in id order",
        "description": "Accounts are append-only and hive_id is deterministic, so a replica keys them on hive_id; id is only the paging cursor.",
        "operationId": "proxy_whitelist_endpoints.stream_accounts",
        "parameters": [
          {
            "name": "after_id",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "default": 0
            },
            "description": "return accounts with a greater id"
          },
          {
            "name": "limit",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "maximum": 100000
            },
            "description": "page size (PostgREST's row limit); at most 100000 rows are returned"
          }
        ],
        "responses": {
          "200": {
            "description": "Accounts ordered by id",
            "headers": {
              "Stream-To-Block": {
                "schema": {
                  "type": "integer"
                },
                "description": "the upper block the page was read with"
              },
              "Stream-Sync-Uuid": {
                "schema": {
                  "type": "string",
                  "format": "uuid"
                },
                "description": "source identity, as in /sync-status"
              }
            },
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "id": {
                        "type": "integer"
                      },
                      "hive_id": {
                        "type": "integer"
                      },
                      "name": {
                        "type": "string"
                      }
                    }
                  }
                }
              }
            }
          },
          "400": {
            "description": "from_block is negative or to_block is lower than from_block"
          }
        }
      }
    },
    "/stream/urls": {
      "get": {
        "tags": [
          "Streams"
        ],
        "summary": "Page of whitelisted URLs ordered by (first_block, md5)",
        "operationId": "proxy_whitelist_endpoints.stream_urls",
        "parameters": [
          {
            "name": "from_block",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "default": 0
            },
            "description": "first block of the range, and the block of the after_* cursor"
          },
          {
            "name": "to_block",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer"
            },
            "description": "last block of the range; defaults to, and is capped at, the last processed block"
          },
          {
            "name": "after_md5",
            "in": "query",
            "required": false,
            "schema": {
              "type": "string"
            },
            "description": "resume after this md5 (lowercase hex) within from_block"
          },
          {
            "name": "limit",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "maximum": 100000
            },
            "description": "page size (PostgREST's row limit); at most 100000 rows are returned"
          }
        ],
        "responses": {
          "200": {
            "description": "URLs with the block that introduced them",
            "headers": {
              "Stream-To-Block": {
                "schema": {
                  "type": "integer"
                },
                "description": "the upper block the page was read with"
              },
              "Stream-Sync-Uuid": {
                "schema": {
                  "type": "string",
                  "format": "uuid"
                },
                "description": "source identity, as in /sync-status"
              }
            },
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "first_block": {
                        "type": "integer"
                      },
                      "md5": {
                        "type": "string"
                      },
                      "url": {
                        "type": "string"
                      }
                    }
                  }
                }
              }
            }
          },
          "400": {
            "description": "from_block is negative or to_block is lower than from_block"
          }
        }
      }
    },
    "/stream/url-authors": {
      "get": {
        "tags": [
          "Streams"
        ],
        "summary": "Page of URL-author associations ordered by (first_block, url_md5, account_name)",
        "operationId": "proxy_whitelist_endpoints.stream_url_authors",
        "parameters": [
          {
            "name": "from_block",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "default": 0
            },
            "description": "first block of the range, and the block of the after_* cursor"
          },
          {
            "name": "to_block",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer"
            },
            "description": "last block of the range; defaults to, and is capped at, the last processed block"
          },
          {
            "name": "after_md5",
            "in": "query",
            "required": false,
            "schema": {
              "type": "string"
            },
            "description": "resume after this URL md5 within from_block"
          },
          {
            "name": "after_account",
            "in": "query",
            "required": false,
            "schema": {
              "type": "string"
            },
            "description": "with after_md5: resume after this account within that URL; without it the whole URL is skipped"
          },
          {
            "name": "limit",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "maximum": 100000
            },
            "description": "page size (PostgREST's row limit); at most 100000 rows are returned"
          }
        ],
        "responses": {
          "200": {
            "description": "Associations with the block that introduced them",
            "headers": {
              "Stream-To-Block": {
                "schema": {
                  "type": "integer"
                },
                "description": "the upper block the page was read with"
              },
              "Stream-Sync-Uuid": {
                "schema": {
                  "type": "string",
                  "format": "uuid"
                },
                "description": "source identity, as in /sync-status"
              }
            },
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "first_block": {
                        "type": "integer"
                      },
                      "url_md5": {
                        "type": "string"
                      },
                      "account_name": {
                        "type": "string"
                      }
                    }
                  }
                }
              }
            }
          },
          "400": {
            "description": "from_block is negative or to_block is lower than from_block"
          }
        }
      }
    },
    "/stream/profiles": {
      "get": {
        "tags": [
          "Streams"
        ],
        "summary": "Page of account profiles ordered by (updated_block, account_name)",
        "description": "Only the current value of each profile is kept: a changed profile moves to its new block. A row with both URLs null means the account removed them.",
        "operationId": "proxy_whitelist_endpoints.stream_profiles",
        "parameters": [
          {
            "name": "from_block",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "default": 0
            },
            "description": "first block of the range, and the block of the after_* cursor"
          },
          {
            "name": "to_block",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer"
            },
            "description": "last block of the range; defaults to, and is capped at, the last processed block"
          },
          {
            "name": "after_account",
            "in": "query",
            "required": false,
            "schema": {
              "type": "string"
            },
            "description": "resume after this account within from_block"
          },
          {
            "name": "limit",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "maximum": 100000
            },
            "description": "page size (PostgREST's row limit); at most 100000 rows are returned"
          }
        ],
        "responses": {
          "200": {
            "description": "Profiles with the block of their last change",
            "headers": {
              "Stream-To-Block": {
                "schema": {
                  "type": "integer"
                },
                "description": "the upper block the page was read with"
              },
              "Stream-Sync-Uuid": {
                "schema": {
                  "type": "string",
                  "format": "uuid"
                },
                "description": "source identity, as in /sync-status"
              }
            },
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "updated_block": {
                        "type": "integer"
                      },
                      "account_name": {
                        "type": "string"
                      },
                      "avatar_url": {
                        "type": [
                          "string",
                          "null"
                        ]
                      },
                      "cover_url": {
                        "type": [
                          "string",
                          "null"
                        ]
                      }
                    }
                  }
                }
              }
            }
          },
          "400": {
            "description": "from_block is negative or to_block is lower than from_block"
          }
        }
      }
    },
    "/stream/blacklists": {
      "get": {
        "tags": [
          "Streams"
        ],
        "summary": "The complete URL and account blacklists",
        "description": "Operator data, small and not chain-derived, so served whole.",
        "operationId": "proxy_whitelist_endpoints.stream_blacklists",
        "responses": {
          "200": {
            "description": "Both blacklists",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "urls": {
                      "type": "array",
                      "items": {
                        "type": "string"
                      }
                    },
                    "accounts": {
                      "type": "array",
                      "items": {
                        "type": "string"
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/avatar/{name}": {
      "get": {
        "tags": [
          "Profiles"
        ],
        "summary": "The account's avatar URL",
        "description": "The profile_image of the account's metadata (posting_json_metadata first, then json_metadata), as a JSON string, or null when the account has none.",
        "operationId": "proxy_whitelist_endpoints.avatar",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string"
            },
            "description": "Hive account name"
          }
        ],
        "responses": {
          "200": {
            "description": "URL or null",
            "headers": {
              "Cache-Control": {
                "schema": {
                  "type": "string"
                },
                "description": "public, max-age=60"
              }
            },
            "content": {
              "application/json": {
                "schema": {
                  "type": [
                    "string",
                    "null"
                  ]
                }
              }
            }
          },
          "404": {
            "description": "The account is unknown to this app"
          }
        }
      }
    },
    "/cover/{name}": {
      "get": {
        "tags": [
          "Profiles"
        ],
        "summary": "The account's cover URL",
        "description": "The cover_image of the account's metadata (posting_json_metadata first, then json_metadata), as a JSON string, or null when the account has none.",
        "operationId": "proxy_whitelist_endpoints.cover",
        "parameters": [
          {
            "name": "name",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string"
            },
            "description": "Hive account name"
          }
        ],
        "responses": {
          "200": {
            "description": "URL or null",
            "headers": {
              "Cache-Control": {
                "schema": {
                  "type": "string"
                },
                "description": "public, max-age=60"
              }
            },
            "content": {
              "application/json": {
                "schema": {
                  "type": [
                    "string",
                    "null"
                  ]
                }
              }
            }
          },
          "404": {
            "description": "The account is unknown to this app"
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
            "description": "Last block processed and its timestamp. last_block_time is null if no block has been processed yet. While the HAF instance is still in massive sync (indexes not yet built) the call fails fast with an error rather than executing an unindexed lookup.",
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
                    },
                    "sync_uuid": {
                      "type": "string",
                      "format": "uuid",
                      "description": "identity of this source database, minted on the first install and kept by reinstalls; a replica re-bootstraps when it changes"
                    },
                    "stream_schema_version": {
                      "type": "integer",
                      "description": "version of the /stream/* contract"
                    },
                    "stream_sentinel_block": {
                      "type": "integer",
                      "description": "the block carried by rows that predate the stream block columns"
                    }
                  }
                },
                "example": {
                  "last_block_num": 5000000,
                  "last_block_time": "2016-09-15T19:47:21",
                  "sync_uuid": "0b5f8f0e-6c3e-4a51-9a49-3f2f0c1d7e21",
                  "stream_schema_version": 1,
                  "stream_sentinel_block": 4999000
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
