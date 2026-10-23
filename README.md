# OpenCDMP CKAN deposit service

**repository-deposit-ckan** is an [OpenCDMP](https://github.com/OpenCDMP/OpenCDMP) deposit service that publishes plans to a [CKAN](https://ckan.org/) instance. It creates a CKAN dataset with the plan's metadata and files and returns the dataset's name, which OpenCDMP stores with the plan as its identifier. CKAN does not mint DOIs.

The service implements the [repository-deposit-base](https://github.com/OpenCDMP/repository-deposit-base) contract. See [Deposit services](https://opencdmp.github.io/optional-services/deposit-services/) in the OpenCDMP documentation.

## Features

- Deposits a plan as a new CKAN dataset, or as a new version linked to the dataset deposited earlier
- Publishes under the organisation's account or the user's own account, with a CKAN API token
- Uploads the plan document (PDF), the machine-actionable plan (RDA JSON) and the supporting files (ZIP)
- Lets users review and edit the dataset's metadata before it is published, with search in CKAN's licences and organizations
- Writes custom dataset fields for CKAN instances with their own dataset schema
- Fills CKAN fields from the answers of the plan blueprint and asset templates, through [semantics](https://opencdmp.github.io/optional-services/deposit-services/ckan-semantics/)

## Requirements

- CKAN 2.11.4, and a CKAN account with an API token that is a member of the CKAN organization datasets are stored in
- An OpenCDMP 2.0 installation, and its Keycloak realm with the `plugins` client scope
- To build from source: Java 21 and Maven. The service is a Spring Boot 4.0 application built on `repository-deposit-base`; the exact versions are in [`pom.xml`](pom.xml).

## Run with Docker

The image is [`opencdmp/repository-deposit-ckan`](https://hub.docker.com/r/opencdmp/repository-deposit-ckan) on Docker Hub. It listens on port `8080`, reads its settings from `/config/app-<PROFILE>.env` and writes its logs to `/logs`:

```bash
docker run -d --name opencdmp-ckan \
  --network <opencdmp-network> \
  -e PROFILE=production \
  -v /srv/opencdmp/ckan/config:/config \
  -v /srv/opencdmp/ckan/logs:/logs \
  opencdmp/repository-deposit-ckan:<tag>
```

Use the tag that the [changelog](https://opencdmp.github.io/deployment/install/changelog/) lists for your OpenCDMP release. Keep the port on the internal network: the `/actuator` endpoints are served without authentication. For a Docker Compose entry, see [Registering plugin services](https://opencdmp.github.io/deployment/configuration/plugin-services/#step-1-run-the-service).

## Build from source

```bash
mvn clean package
docker build -t repository-deposit-ckan:local .
```

Maven builds `web/target/repository-deposit-web-1.0.0-SNAPSHOT.jar`, and the `Dockerfile` builds the service from source on its own. The service contacts Keycloak at `IDP_ISSUER_URI` when it starts, and does not start if it cannot reach it. To run the JAR against a local OpenCDMP installation, see [Run and test a plugin locally](https://opencdmp.github.io/developers/plugins/run-locally/).

## Configuration

The service reads its settings from environment variables or from `app-<PROFILE>.env`. The main ones:

| Variable | Description |
| --- | --- |
| `IDP_ISSUER_URI` | Keycloak realm URL, the same value as in the OpenCDMP API service |
| `IDP_CLAIMS_ROLES_PATH` | `plugins.roles` |
| `SECURITY_AUDIENCE` | `plugins` |
| `OPENCDMP_DOMAIN` | Address of the OpenCDMP installation, with a trailing slash |
| `DEPOSIT_CKAN_URL` | CKAN action API URL, such as `https://ckan.example.org/api/3/action/` |
| `DEPOSIT_CKAN_RECORD_URL` | Address of a deposited dataset, with `{doi_id}` in place of its name |
| `DEPOSIT_CKAN_ACCESS_TOKEN` | API token of the installation's CKAN account |
| `DEPOSIT_CKAN_ORGANIZATION_ID` | CKAN organization that datasets are stored in |
| `DEPOSIT_CKAN_SUPPORTS_PREVIEW` | Metadata review before publishing (default `true`) |
| `STORAGE_PATH` | Folder of temporary and shared files |

All variables, with defaults and examples, are in [Deposit services: CKAN](https://opencdmp.github.io/deployment/configuration/backend/deposit/#ckan); the authentication, logging and storage settings are in [Settings common to all plugin services](https://opencdmp.github.io/deployment/configuration/plugin-services/#settings-common-to-all-plugin-services). Each organisation can override the account and the record defaults in its [organisation settings](https://opencdmp.github.io/optional-services/deposit-services/#organisation-settings).

### Publishing accounts

For each deposit, the service uses the first of these CKAN tokens that is set:

1. The API token the user saved in their [profile](https://opencdmp.github.io/using/profile-settings/#repository-accounts) or typed in the publish dialog (`authInfo.authFields`, code `ckan-access-token`)
2. The organisation's **Access token** setting
3. `DEPOSIT_CKAN_ACCESS_TOKEN`

OpenCDMP decides which accounts a user may publish under, and never falls back to the organisation's or the installation's account when a user chose their own. See [Publishing accounts and files](https://opencdmp.github.io/optional-services/deposit-services/#publishing-accounts-and-files).

### The account a deposit used

After a deposit under a user's own account, OpenCDMP calls `POST /api/deposit/account` with the same credentials, and the service reads the token's CKAN user from `GET <DEPOSIT_CKAN_URL>user_show` without an `id`, which CKAN 2.10 and later answer with the token's own user. It returns the user's id and a name to show: the full name, otherwise the username, with the email when CKAN returns one. OpenCDMP keeps them with the dataset's name and names that account when a later version is published, since an account that cannot see or edit the earlier dataset may be refused. It never asks for the organisation's account, so that account's CKAN identity is not stored or shown to users.

The service returns nothing when CKAN does not answer, and the deposit is not affected; OpenCDMP then shows its generic message.

### Custom dataset fields

A CKAN instance with its own dataset schema can receive extra fields. Declare them in `custom-fields`, for example in `/config/custom-fields-<PROFILE>.yml`:

```yaml
custom-fields:
  - code: dataset_category      # semantic ckan.custom_fields.dataset_category
    jPath: dataset_category     # the field in the CKAN dataset
    dataType: String            # String | Boolean | Integer | Long | Double
    isArray: true
```

A blueprint or asset template field fills a custom field through the semantic `ckan.custom_fields.<code>`. See [CKAN semantics: custom fields](https://opencdmp.github.io/optional-services/deposit-services/ckan-semantics/#custom-fields).

## Register with OpenCDMP

Register the service for the whole installation in the configuration of the API service, or for one organisation under **Settings > Plugins > Add plugin**. Use the plugin id `ckan`, and register a PDF and an RDA JSON transformer with it. See [Registering plugin services](https://opencdmp.github.io/deployment/configuration/plugin-services/) and [Organisation plugins](https://opencdmp.github.io/administering/tenant-management/plugins/).

## API

All endpoints are under `/api/deposit`. They accept only a Bearer token from the installation's Keycloak realm that carries the `plugins` audience; the OpenCDMP API service obtains one for each call. See [Let the plugin accept the API service's tokens](https://opencdmp.github.io/deployment/configuration/plugin-services/#plugin-tokens).

| Endpoint | Description |
| --- | --- |
| `POST /api/deposit` | Deposit a plan and return the dataset's name |
| `POST /api/deposit/preview` | Return the metadata that would be deposited, as an editable form |
| `GET /api/deposit/vocab/{fieldCode}?like=` | Search the vocabulary behind a review field |
| `POST /api/deposit/account` | Return the CKAN account that a deposit's credentials belong to (see [The account a deposit used](#the-account-a-deposit-used)) |
| `GET /api/deposit/configuration` | Return the service's `DepositConfiguration` |
| `GET /api/deposit/logo` | Return the CKAN logo, Base64-encoded |

A deposit takes a `PlanDepositModel`. For a new version of a dataset deposited earlier, `previousDOI` holds the dataset's name:

```json
{
  "planModel": {
    "id": "<plan-id>",
    "label": "My research plan",
    "previousDOI": "<name-of-the-earlier-dataset>"
  },
  "authInfo": {
    "authFields": [
      { "code": "ckan-access-token", "textValue": "<user-api-token>" }
    ]
  }
}
```

The models are described in the [Deposit plugin guide](https://opencdmp.github.io/developers/plugins/deposit/#key-interfaces-and-classes) and [Common models](https://opencdmp.github.io/developers/plugins/common-models/).

## Deposit preview

The service reports `supportsPreview: true` in its configuration unless `DEPOSIT_CKAN_SUPPORTS_PREVIEW` is `false`. OpenCDMP then shows users the dataset's metadata in a form before anything is submitted to CKAN:

- `POST /api/deposit/preview` takes the same `PlanDepositModel` as a deposit; `authInfo` is not needed. It runs the same mapping as a deposit and returns a `DepositPreviewModel`: the form, with the mapped values filled in. Overrides saved earlier, in the optional `storedFields` and `storedGroups`, are merged in.
- `POST /api/deposit` applies the user's `editedFields` (single values) and `editedGroups` (repeatable groups) to the dataset just before it is submitted.

The contract is described in [Deposit preview and metadata review](https://opencdmp.github.io/developers/plugins/deposit-preview/).

### The form

Fields are Mapped (filled in from the plan and editable, such as `title`, `notes` and `tags`), Manual (empty for the user to complete, such as the fields declared in `preview-extra-fields`) or System (read-only, such as `name`).

| Group code | Contents |
|---|---|
| `files` | `files` (read-only): the names of the files the deposit will upload, when the platform provides them |
| `basic-information` | `title`, `notes`, `version`, `url`, `tags`, `name` (read-only identifier) |
| `access` | `license` (vocabulary autocomplete), `owner-org` (vocabulary autocomplete), `private` |
| `people` | `author`, `author-email`, `maintainer`, `maintainer-email` |
| `creators` | repeatable rows: `given-name`, `family-name`, `orcid`; only present when one of the numbered creator fields is declared (see below) |
| `extras` | repeatable rows: `extra-key`, `extra-value` |
| `domain-specific` | Manual fields defined by the installation in `preview-extra-fields` (see below) |

Field codes are unique across the form, and the service ignores codes it does not know. An organisation can hide groups and lock record defaults in its settings.

### Installation-specific fields

CKAN has no generic API that lists an instance's dataset schema, so declare the instance's own fields in configuration, next to `custom-fields`. They appear as Manual fields in the "Domain specific fields" group, and their values are written as they are, as custom dataset fields next to the built-in ones:

```yaml
preview-extra-fields:
    fields:
        - key: "spatial_coverage"     # the custom field key; also the preview field code
          label: "Spatial coverage"
          type: text                   # text | textarea | tags | boolean | number | date
```

The custom fields `creator_first_name`, `creator_last_name` and `creator_orcid_id` hold one entry per creator, under numbered keys: `creator_first_name_1`, `creator_last_name_1`, `creator_orcid_id_1`, `creator_first_name_2`, and so on. The form shows them as the repeatable `creators` group, one row per creator, and the deposit accepts edits to that group, only when at least one of the three codes is declared in `preview-extra-fields`. One is enough: the group then shows and writes all three. The three codes never appear in the "Domain specific fields" group.

### How edits apply

- A blank edit to a single value never erases a mapped value.
- An edited repeatable group replaces the whole list.
- For a new version (`previousDOI` set), the service sets `version` itself, whatever the user submits: the previous dataset's version plus one, or the plan's version when the previous one is not a number.
- Saved overrides are merged into the preview by `DepositPreviewMergeHelper` from repository-deposit-base, with the [shared merge rules](https://opencdmp.github.io/developers/plugins/deposit-preview/#depositpreviewmergehelper).

### Vocabulary search

`GET /api/deposit/vocab/{fieldCode}?like=<term>` returns autocomplete options for the review fields marked `vocabSearch`. `license` is answered from CKAN's `license_list` action and `owner-org` from `organization_list`; any other code returns an empty list.

## Related resources

- [Deposit services overview](https://opencdmp.github.io/optional-services/deposit-services/)
- [CKAN semantics reference](https://opencdmp.github.io/optional-services/deposit-services/ckan-semantics/)
- [Deposit services configuration](https://opencdmp.github.io/deployment/configuration/backend/deposit/#ckan)
- [Registering plugin services](https://opencdmp.github.io/deployment/configuration/plugin-services/)
- [Deposit a plan](https://opencdmp.github.io/using/plans/deposit-a-plan/): the user guide
- [Deposit plugin guide](https://opencdmp.github.io/developers/plugins/deposit/): building your own deposit service

## Contributing

Report bugs and suggest changes in [GitHub Issues](https://github.com/OpenCDMP/repository-deposit-ckan/issues). See [Contributing](https://opencdmp.github.io/developers/contributing/) for how to propose a change.

## License

repository-deposit-ckan is licensed under the [European Union Public Licence (EUPL) v1.2](LICENSE).

## Contact

Questions: email opencdmp at cite.gr.
