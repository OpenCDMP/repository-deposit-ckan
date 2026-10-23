package org.opencdmp.deposit.ckan.service.ckan;

import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.util.MultiValueMap;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.client.BufferingClientHttpRequestFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpEntity;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.opencdmp.deposit.ckan.validation.CkanErrors;
import org.opencdmp.deposit.ckan.validation.CkanDatasetValidator;
import org.opencdmp.deposit.ckan.configuration.preview.PreviewExtraFieldsProperties;
import gr.cite.tools.exception.MyValidationException;
import gr.cite.tools.exception.MyApplicationException;
import gr.cite.tools.logging.LoggerService;
import gr.cite.tools.logging.MapLogEntry;
import org.json.JSONObject;
import org.opencdmp.commonmodels.models.FileEnvelopeModel;
import org.opencdmp.commonmodels.models.deposit.DepositPreviewModel;
import org.opencdmp.commonmodels.models.deposit.PreviewFieldOption;
import org.opencdmp.commonmodels.models.plan.PlanModel;
import org.opencdmp.commonmodels.models.plugin.PluginUserFieldModel;
import org.opencdmp.deposit.ckan.configuration.CkanSemanticsManifest;
import org.opencdmp.deposit.ckan.configuration.SemanticsProperties;
import org.opencdmp.deposit.ckan.model.CkanDataset;
import org.opencdmp.deposit.ckan.model.DatasetRelationship;
import org.opencdmp.deposit.ckan.model.builder.CkanBuilder;
import org.opencdmp.deposit.ckan.model.builder.CkanPreviewMapper;
import org.opencdmp.deposit.ckan.service.storage.FileStorageService;
import org.opencdmp.depositbase.repository.DepositAccount;
import org.opencdmp.depositbase.repository.DepositConfiguration;
import org.opencdmp.depositbase.repository.DepositPreviewMergeHelper;
import org.opencdmp.depositbase.repository.DepositPluginSettings;
import org.opencdmp.depositbase.repository.PlanDepositModel;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.*;

@Component
public class CkanDepositServiceImpl implements CkanDepositService {
    private static final LoggerService logger = new LoggerService(LoggerFactory.getLogger(CkanDepositServiceImpl.class));

    private static final String CONFIGURATION_FIELD_ACCESS_TOKEN = "ckan-access-token";
    private static final String SETTING_ORGANISATION_ACCESS_TOKEN = "organisation-access-token";
    private static final String DERIVES_FROM = "derives_from";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final CkanServiceProperties ckanServiceProperties;
    private final CkanBuilder ckanBuilder;
    private final CkanPreviewMapper ckanPreviewMapper;
    private final FileStorageService storageService;
    private final ResourceLoader resourceLoader;
    private final CkanDatasetValidator datasetValidator;
    private final PreviewExtraFieldsProperties previewExtraFieldsProperties;

    private byte[] logo;;
    
    @Autowired
    public CkanDepositServiceImpl(CkanServiceProperties ckanServiceProperties, CkanBuilder mapper, FileStorageService storageService, ResourceLoader resourceLoader, SemanticsProperties semanticsProperties, LocalCustomFieldProperties customFieldProperties, CkanPreviewMapper ckanPreviewMapper, CkanDatasetValidator datasetValidator, PreviewExtraFieldsProperties previewExtraFieldsProperties){
        this.ckanServiceProperties = ckanServiceProperties;
        this.ckanBuilder = mapper;
	    this.storageService = storageService;
        this.resourceLoader = resourceLoader;
        this.ckanPreviewMapper = ckanPreviewMapper;
        this.datasetValidator = datasetValidator;
        this.previewExtraFieldsProperties = previewExtraFieldsProperties;
        this.ckanServiceProperties.getDepositConfiguration().setSemantics(CkanSemanticsManifest.build(semanticsProperties, customFieldProperties));
        this.logo = null;
    }

    @Override
    public DepositPreviewModel preview(PlanDepositModel planDepositModel) {
        if (planDepositModel == null || planDepositModel.getPlanModel() == null) return null;

        CkanDataset dataset = this.ckanBuilder.build(planDepositModel.getPlanModel());
        List<String> uploadFileNames = new ArrayList<>();
        PlanModel planModel = planDepositModel.getPlanModel();
        if (planModel.getPdfFile() != null && planModel.getPdfFile().getFilename() != null) uploadFileNames.add(planModel.getPdfFile().getFilename());
        if (planModel.getRdaJsonFile() != null && planModel.getRdaJsonFile().getFilename() != null) uploadFileNames.add(planModel.getRdaJsonFile().getFilename());
        if (planModel.getSupportingFilesZip() != null && planModel.getSupportingFilesZip().getFilename() != null) uploadFileNames.add(planModel.getSupportingFilesZip().getFilename());
        DepositPreviewModel preview = this.ckanPreviewMapper.toPreview(dataset, uploadFileNames, DepositPluginSettings.of(planDepositModel, this.getConfiguration()));
        DepositPreviewMergeHelper.merge(preview, planDepositModel.getStoredFields(), planDepositModel.getStoredGroups());
        return preview;
    }

    @Override
    public List<PreviewFieldOption> searchVocabulary(String fieldCode, String like) {
        return this.ckanPreviewMapper.searchVocabulary(fieldCode, like);
    }

    @Override
    public String deposit(PlanDepositModel planDepositModel) {

        DepositConfiguration depositConfiguration = this.getConfiguration();

        if(depositConfiguration != null && planDepositModel != null && planDepositModel.getPlanModel() != null) {

            String token = this.ckanToken(planDepositModel, depositConfiguration);
            String previousDOI = planDepositModel.getPlanModel().getPreviousDOI();

            try {

                if (previousDOI == null) {
                    return depositFirst(planDepositModel, token);
                } else {
                    return depositNewVersion(planDepositModel, token);
                }

            } catch (WebClientResponseException ex) {
                throw this.toException(ex, ex.getStatusCode().is4xxClientError(), ex.getResponseBodyAsString(), ex.getStatusText(), null);
            } catch (HttpStatusCodeException ex) {
                throw this.toException(ex, ex.getStatusCode().is4xxClientError(), ex.getResponseBodyAsString(), ex.getStatusText(), null);
            }

        }

        return null;

    }


    private String depositFirst(PlanDepositModel planDepositModel, String token)  {
        PlanModel planModel = planDepositModel.getPlanModel();
        CkanDataset dataset = this.ckanBuilder.build(planModel);
        this.ckanPreviewMapper.applyOverrides(dataset, planDepositModel.getEditedFields(), planDepositModel.getEditedGroups(), DepositPluginSettings.of(planDepositModel, this.getConfiguration()));

        Map<String, Object> response = this.createDataset(dataset, token);
        String id = String.valueOf(response.get("id"));
        try {
            this.uploadFiles(planModel, id, token);
        } catch (RuntimeException ex) {
            this.deleteDataset(id, token);
            throw ex;
        }
        this.sendRelationshipFromSemantics(planModel, id, token);
        return String.valueOf(response.get("name"));
    }

    /** Checks the dataset against CKAN's core schema, then creates it; a refusal is reported with the review's labels. */
    private Map<String, Object> createDataset(CkanDataset dataset, String token) {
        Map<String, List<String>> problems = this.datasetValidator.validate(dataset);
        if (!problems.isEmpty()) throw CkanErrors.exception(CkanErrors.INVALID_VALUES, problems);

        String url = this.ckanServiceProperties.getDepositConfiguration().getRepositoryUrl() + "package_create";
        Map<String, Object> response;
        try {
            response = this.getWebClient().post().uri(url).headers(httpHeaders -> {
                        httpHeaders.set("Authorization", token);
                        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
                    })
                    .bodyValue(dataset).exchangeToMono(mono ->
                            mono.statusCode().isError() ?
                                    mono.createException().flatMap(Mono::error) :
                                    mono.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})).block();
        } catch (WebClientResponseException ex) {
            throw this.toException(ex, ex.getStatusCode().is4xxClientError(), ex.getResponseBodyAsString(), ex.getStatusText(), dataset);
        }
        if (response == null || !(response.get("result") instanceof Map<?, ?> result)) throw new MyApplicationException("The repository did not return the created dataset");
        return (Map<String, Object>) result;
    }

    /** A dataset whose files could not be added is removed: purged when the account may, otherwise deleted. */
    private void deleteDataset(String id, String token) {
        String baseUrl = this.ckanServiceProperties.getDepositConfiguration().getRepositoryUrl();
        for (String action : List.of("dataset_purge", "package_delete")) {
            try {
                this.getWebClient().post().uri(baseUrl + action).headers(httpHeaders -> {
                            httpHeaders.set("Authorization", token);
                            httpHeaders.setContentType(MediaType.APPLICATION_JSON);
                        })
                        .bodyValue(Map.of("id", id)).retrieve().toBodilessEntity().block();
                return;
            } catch (Exception e) {
                logger.warn(new MapLogEntry("Could not remove the dataset").And("action", action).And("id", id).And("error", e.getMessage()));
            }
        }
    }

    /** What CKAN answered, as a validation error when it refused the values and otherwise as it said it. */
    private RuntimeException toException(Exception ex, boolean clientError, String body, String statusText, CkanDataset dataset) {
        logger.error(ex.getMessage(), ex);
        JsonNode error = null;
        try {
            error = objectMapper.readTree(body).path("error");
        } catch (Exception ignored) {
            // not a JSON answer
        }
        if (CkanErrors.isValidationError(error)) {
            List<String> extraKeys = dataset != null && dataset.getExtras() != null ? dataset.getExtras().stream().map(extra -> extra != null ? extra.getKey() : null).toList() : null;
            Map<String, List<String>> problems = CkanErrors.fromCkan(error, this::customFieldLabel, extraKeys);
            if (!problems.isEmpty()) return CkanErrors.exception(CkanErrors.INVALID_VALUES, problems);
        }
        String message = CkanErrors.message(error);
        if (message == null || message.isBlank()) message = statusText;
        if (clientError) return new MyValidationException("The repository refused the deposit: " + message);
        return new MyApplicationException("The repository could not take the deposit: " + message);
    }

    private String customFieldLabel(String key) {
        if (this.previewExtraFieldsProperties.getFields() == null) return null;
        return this.previewExtraFieldsProperties.getFields().stream().filter(field -> field != null && key.equals(field.getKey())).map(PreviewExtraFieldsProperties.ExtraField::getLabel).findFirst().orElse(null);
    }

    private void uploadFiles(PlanModel planModel, String id, String token) {
        if (planModel.getPdfFile() != null) this.uploadFile(planModel.getPdfFile(), id, token);
        if (planModel.getRdaJsonFile() != null) this.uploadFile(planModel.getRdaJsonFile(), id, token);
        if (planModel.getSupportingFilesZip() != null) this.uploadFile(planModel.getSupportingFilesZip(), id, token);
    }

    private void uploadFile(FileEnvelopeModel fileEnvelopeModel, String datasetId, String token) {

        if (fileEnvelopeModel == null) return;

        byte[] fileBytes = null;
        if (this.getConfiguration().isUseSharedStorage() && fileEnvelopeModel.getFileRef() != null && !fileEnvelopeModel.getFileRef().isBlank()) {
            fileBytes = this.storageService.readFile(fileEnvelopeModel.getFileRef());
        }
        if (fileBytes == null || fileBytes.length == 0){
            fileBytes = fileEnvelopeModel.getFile();
        }

        String url = this.ckanServiceProperties.getDepositConfiguration().getRepositoryUrl() + "resource_create";

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", token);
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add("package_id", datasetId);
        body.add("name", fileEnvelopeModel.getFilename());
        body.add("upload", new ByteArrayResource(fileBytes) {
            @Override
            public String getFilename() {
                return fileEnvelopeModel.getFilename();
            }
        });

        // CKAN served by werkzeug does not read a chunked body, so the form is sent whole, with its length
        RestTemplate restTemplate = new RestTemplate(new BufferingClientHttpRequestFactory(new SimpleClientHttpRequestFactory()));
        restTemplate.postForEntity(url, new HttpEntity<>(body, headers), String.class);
    }

    private void sendRelationshipFromSemantics(PlanModel planModel, String datasetId, String token) throws JacksonException {
        List<DatasetRelationship> relationships = ckanBuilder.buildDatasetRelationshipBySemantics(planModel, datasetId);

        for (DatasetRelationship relationship: relationships) {
            String url = this.ckanServiceProperties.getDepositConfiguration().getRepositoryUrl() + "package_relationship_create";

            try {
                this.getWebClient().post().uri(url).headers(httpHeaders -> {
                            httpHeaders.set("Authorization", token);
                            httpHeaders.setContentType(MediaType.APPLICATION_JSON);
                        })
                        .bodyValue(relationship).exchangeToMono(mono ->
                                mono.statusCode().isError() ?
                                        mono.createException().flatMap(Mono::error) :
                                        mono.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})).block();
            } catch (Exception e) {
                logger.warn("Wrong relationship input: " + objectMapper.writeValueAsString(relationship));
                logger.warn(e.getMessage());
            }
        }
    }

    private String depositNewVersion(PlanDepositModel planDepositModel, String token) {
        PlanModel planModel = planDepositModel.getPlanModel();
        JsonNode oldDatasetJson = this.getDatasetIdentifier(planModel.getPreviousDOI(), token).get(0);
        String oldDatasetId = oldDatasetJson.get("id").asText();
        String oldVersion = oldDatasetJson.hasNonNull("version") ? oldDatasetJson.get("version").asText() : null;

        Map<String, Object> newDataset = this.updateVersion(planDepositModel, oldVersion, token);

        if (newDataset != null && newDataset.get("id") != null) {
            try {
                this.uploadFiles(planModel, newDataset.get("id").toString(), token);
                this.createNewVersionRelationship(oldDatasetId, newDataset.get("id").toString(), token);
            } catch (RuntimeException ex) {
                this.deleteDataset(newDataset.get("id").toString(), token);
                throw ex;
            }
            this.sendRelationshipFromSemantics(planModel, newDataset.get("id").toString(), token);
            return (String) newDataset.get("name");
        }
        return null;
    }

    private JsonNode getDatasetIdentifier(String previousDOI, String token) throws JacksonException {
        String url = this.ckanServiceProperties.getDepositConfiguration().getRepositoryUrl() + "package_search?q=name:" + previousDOI + "&include_private=True";

        Map<String, Object> response = this.getWebClient().get().uri(url).headers(httpHeaders -> {
            httpHeaders.set("Authorization", token);
            httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        }).retrieve().toEntity(Map.class).block().getBody();

        if (response == null) return null;

        JsonNode jsonNode = objectMapper.readTree(new JSONObject(response).toString()).get("result");
        return jsonNode.findValues("results").getFirst();
    }

    private Map<String, Object> updateVersion(PlanDepositModel planDepositModel, String version, String token){

        PlanModel planModel = planDepositModel.getPlanModel();
        CkanDataset dataset = this.ckanBuilder.build(planModel);
        this.ckanPreviewMapper.applyOverrides(dataset, planDepositModel.getEditedFields(), planDepositModel.getEditedGroups(), DepositPluginSettings.of(planDepositModel, this.getConfiguration()));
        // the version is system-managed for new-version deposits (auto-incremented), never user-editable here
        dataset.setVersion(this.nextVersion(version, planModel));

        return this.createDataset(dataset, token);
    }

    private String nextVersion(String version, PlanModel planModel) {
        try {
            return String.valueOf(Integer.parseInt(version.trim()) + 1);
        } catch (RuntimeException e) {
            // the previous record's version was not a number (e.g. edited in the review): use the plan's
            logger.warn("Previous CKAN version is not numeric: " + version);
            return String.valueOf(planModel.getVersion());
        }
    }

    private void createNewVersionRelationship(String oldDatasetId, String newDatasetId, String token){

        DatasetRelationship relationship = new DatasetRelationship();
        relationship.setSubject(newDatasetId);
        relationship.setObject(oldDatasetId);
        relationship.setType(DERIVES_FROM);
        relationship.setComment("New version");

        String url = this.ckanServiceProperties.getDepositConfiguration().getRepositoryUrl() + "package_relationship_create";

        this.getWebClient().post().uri(url).headers(httpHeaders -> {
                    httpHeaders.set("Authorization", token);
                    httpHeaders.setContentType(MediaType.APPLICATION_JSON);
                })
                .bodyValue(relationship).exchangeToMono(mono ->
                        mono.statusCode().isError() ?
                                mono.createException().flatMap(Mono::error) :
                                mono.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})).block();
    }


    @Override
    public DepositConfiguration getConfiguration() {
        return this.ckanServiceProperties.getDepositConfiguration();
    }

    private String ckanToken(PlanDepositModel planDepositModel, DepositConfiguration depositConfiguration) {
        String token = null;
        if (planDepositModel.getAuthInfo() != null) {
            if (planDepositModel.getAuthInfo().getAuthToken() != null && !planDepositModel.getAuthInfo().getAuthToken().isBlank()) token = planDepositModel.getAuthInfo().getAuthToken();
            else if (planDepositModel.getAuthInfo().getAuthFields() != null && !planDepositModel.getAuthInfo().getAuthFields().isEmpty() && depositConfiguration.getUserConfigurationFields() != null) {
                PluginUserFieldModel userFieldModel = planDepositModel.getAuthInfo().getAuthFields().stream().filter(x -> x.getCode().equals(CONFIGURATION_FIELD_ACCESS_TOKEN)).findFirst().orElse(null);
                if (userFieldModel != null && userFieldModel.getTextValue() != null && !userFieldModel.getTextValue().isBlank()) token = userFieldModel.getTextValue();
            }
        }

        if (token == null || token.isBlank()) token = DepositPluginSettings.of(planDepositModel.getPlanModel(), depositConfiguration).text(SETTING_ORGANISATION_ACCESS_TOKEN, this.ckanServiceProperties.getDepositConfiguration().getAccessToken());
        return token;
    }

    @Override
    public DepositAccount account(PlanDepositModel planDepositModel) {
        DepositConfiguration depositConfiguration = this.getConfiguration();
        if (depositConfiguration == null || planDepositModel == null || planDepositModel.getPlanModel() == null) return null;

        String token = this.ckanToken(planDepositModel, depositConfiguration);
        if (token == null || token.isBlank()) return null;

        try {
            // user_show without an id returns the token's own user (CKAN 2.10+); the token goes in the header because the request logger prints URLs
            Map<String, Object> response = this.getWebClient().get().uri(depositConfiguration.getRepositoryUrl() + "user_show").headers(httpHeaders -> {
                httpHeaders.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
                httpHeaders.set("Authorization", token);
            }).exchangeToMono(mono -> mono.statusCode().isError() ? mono.createException().flatMap(Mono::error) : mono.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})).block();
            if (response == null || !(response.get("result") instanceof Map<?, ?> user) || user.get("id") == null) return null;

            String fullName = user.get("fullname") instanceof String value && !value.isBlank() ? value : null;
            String username = user.get("name") instanceof String value && !value.isBlank() ? value : null;
            String email = user.get("email") instanceof String value && !value.isBlank() ? value : null;
            String name = fullName != null ? fullName : username != null ? username : email;
            if (name != null && email != null && !name.equals(email)) name = name + " (" + email + ")";

            DepositAccount account = new DepositAccount();
            account.setId(user.get("id").toString());
            account.setName(name);
            return account;
        } catch (Exception ex) {
            logger.error("could not read the CKAN account of the deposit", ex);
            return null;
        }
    }

    @Override
    public String authenticate(String code){
        return null;
    }

    @Override
    public String getLogo() {
        DepositConfiguration zenodoConfig = this.ckanServiceProperties.getDepositConfiguration();
        if(zenodoConfig != null && zenodoConfig.isHasLogo() && this.ckanServiceProperties.getLogo() != null && !this.ckanServiceProperties.getLogo().isBlank()) {
            if (this.logo == null) {
                try {
                    Resource resource = resourceLoader.getResource(this.ckanServiceProperties.getLogo());
                    if(!resource.isReadable()) return null;
                    try(InputStream inputStream = resource.getInputStream()) {
                        this.logo = inputStream.readAllBytes();
                    }
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }
            return (this.logo != null && this.logo.length != 0) ? Base64.getEncoder().encodeToString(this.logo) : null;
        }
        return null;
    }
    
    private WebClient getWebClient(){
        return WebClient.builder().filters(exchangeFilterFunctions -> {
            exchangeFilterFunctions.add(logRequest());
            exchangeFilterFunctions.add(logResponse());
        }).codecs(codecs -> codecs
                .defaultCodecs()
                .maxInMemorySize(this.ckanServiceProperties.getMaxInMemorySizeInBytes())
        ).build();
    }

    private static ExchangeFilterFunction logRequest() {
        return ExchangeFilterFunction.ofRequestProcessor(clientRequest -> {
            logger.debug(new MapLogEntry("Request").And("method", clientRequest.method().toString()).And("url", clientRequest.url().toString()));
            return Mono.just(clientRequest);
        });
    }

    private static ExchangeFilterFunction logResponse() {
        return ExchangeFilterFunction.ofResponseProcessor(response -> {
            if (response.statusCode().isError()) {
                // reading the body consumes it, so the response passed on gets it back for the caller to read CKAN's error
                return response.bodyToMono(String.class).defaultIfEmpty("")
                    .map(body -> {
                        logger.error(new MapLogEntry("Response").And("method", response.request().getMethod().toString()).And("url", response.request().getURI()).And("status", response.statusCode().toString()).And("body", body));
                        return response.mutate().body(body).build();
                    });
            }
            return Mono.just(response);
            
        });
    }
}
