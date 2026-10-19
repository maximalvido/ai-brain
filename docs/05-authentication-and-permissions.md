# Authentication and Permissions

Status: Implemented for the v0.1 installation-level API key. Per-user enterprise identity is post-alpha.

## Scope

A self-hosted installation represents one company environment. It manages users, teams, roles, service accounts, connector accounts, and source permission mappings. Cloud organizations and multi-tenancy are out of scope for the initial architecture.

## Authentication and authorization

```text
Authentication → verify caller
Identity       → create principal
Authorization  → evaluate access
```

An email address is an identity attribute, not sufficient authentication. In
v0.1, access is protected by one installation-level API key. Per-user identity
and source ACL mapping are post-alpha work.

## Principal

```text
Principal
- type: user | service_account
- id
- email, when applicable
- teams
- roles
- source_identities
- authentication_method
```

The retrieval layer receives a normalized principal and does not need to know whether the caller is using Claude, ChatGPT, Gemini, a custom agent, or a script.

## Usage modes

### Interactive MCP clients

An MCP client authenticates a human user to AI Brain. The model-provider account and Brain identity are separate security domains. The Brain must receive its own verified user identity before returning protected knowledge.

For a locally spawned stdio server, the MCP client launches a process inside the
trusted self-hosted environment. The Brain requires the installation key from
`.env` at startup.

### Programmatic clients

Scripts and remote MCP clients send the installation key as a Bearer token.

## Roles

Initial roles may include:

- administrator;
- connector administrator;
- user;
- auditor;
- service account.

Roles control Brain operations. Source-native permissions control access to source knowledge.

## Source permission mapping

Connectors should import source identities, groups, and resource ACLs where available. The Brain maps them to local users and teams.

Examples:

- Slack channel membership;
- Slack private-channel visibility;
- Linear team and project access;
- source user and group identities;
- deleted or revoked source access.

## Retrieval enforcement

Permission filtering occurs before results leave the Brain:

```text
authenticated principal
        ↓
candidate retrieval
        ↓
permission filtering
        ↓
ranking and evidence selection
        ↓
MCP response
```

Derived knowledge must not reveal information from inaccessible evidence. The initial conservative rule is that a derived result requiring multiple sources is accessible only when the requester can access all required sources.

## API-key rules

The installation key is appropriate for the private v0.1 alpha deployment. It
does not provide per-user auditing or permission enforcement.

`BRAIN_API_KEY` is a deployment secret and should be stored outside source
control. Per-user credentials, revocation, role management, and OIDC/SAML
integration are planned after the alpha proves the ingestion and retrieval
model.

## Security requirements for v0.1

- No unauthenticated protected retrieval.
- The installation API key is injected through environment configuration.
- Retrieval logs include principal and accessed source references.
- Permission changes invalidate affected retrieval projections.
- Authorization failures do not reveal whether a hidden resource exists.
