# Repository Deposit CKAN for OpenCDMP

**repository-deposit-ckan** is an implementation of the `repository-deposit-base` package that enables the deposition of **OpenCDMP Plans** into the **CKAN** repository. This service allows users of the **OpenCDMP** platform to submit their DMPs to CKAN, minting a **Digital Object Identifier (DOI)** for each plan. The service is built using **Spring Boot** and can be easily integrated with OpenCDMP as a repository deposit option.

## Overview

The **CKAN** repository is a widely-used open-access research repository that offers DOI assignment for uploaded content. By using the **repository-deposit-CKAN** service, OpenCDMP users can directly deposit their DMPs into CKAN, making the plans citeable and publicly available. The service supports both **system-based** and **user-based** depositions depending on the configuration.

- **Deposits**: Supported for DMPs into the CKAN repository.
- **DOI Minting**: Each successful deposition will mint a DOI through CKAN.

## Features

- **Plan Deposits**: Deposit OpenCDMP plans into CKAN.
- **DOI Minting**: Automatically mint DOIs for each submitted plan.
- **Spring Boot Microservice**: Built as a Spring Boot microservice for seamless integration with OpenCDMP.

## Key Endpoints

This service implements the following endpoints as per `DepositController`:

### Deposit Endpoint

- **POST `/deposit`**: Deposits a plan into CKAN and returns the DOI.

```bash
POST /deposit
{
    "planDepositModel": { ... },
    "authToken": "user_oauth2_access_token"
}
```

### Configuration Endpoint

- **GET `/configuration`**: Returns the repository's configuration for CKAN.

```bash
GET /configuration
```

### Logo Endpoint

- **GET `/logo`**: Returns the CKAN logo in base64 format if available.

```bash
GET /logo
```

## Example

To deposit a plan into CKAN and mint a DOI:

```bash
POST /deposit
{
    "planDepositModel": { ... },
    "authToken": "user_oauth2_access_token"
}
```

## License

This repository is licensed under the [EUPL 1.2 License](LICENSE).

## Contact

For questions or support regarding this implementation, please contact:

- **Email**: opencdmp at cite.gr
