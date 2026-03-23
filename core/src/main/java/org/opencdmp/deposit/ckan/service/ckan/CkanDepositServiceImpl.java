package org.opencdmp.deposit.ckan.service.ckan;

import gr.cite.tools.logging.LoggerService;
import gr.cite.tools.logging.MapLogEntry;
import org.json.JSONObject;
import org.opencdmp.commonmodels.models.FileEnvelopeModel;
import org.opencdmp.commonmodels.models.plan.PlanModel;
import org.opencdmp.commonmodels.models.plugin.PluginUserFieldModel;
import org.opencdmp.deposit.ckan.model.CkanDataset;
import org.opencdmp.deposit.ckan.model.CkanUploadData;
import org.opencdmp.deposit.ckan.model.DatasetRelationship;
import org.opencdmp.deposit.ckan.model.builder.CkanBuilder;
import org.opencdmp.deposit.ckan.service.storage.FileStorageService;
import org.opencdmp.depositbase.repository.DepositConfiguration;
import org.opencdmp.depositbase.repository.PlanDepositModel;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
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
    private static final String DERIVES_FROM = "derives_from";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final CkanServiceProperties ckanServiceProperties;
    private final CkanBuilder ckanBuilder;
    private final FileStorageService storageService;
    private final ResourceLoader resourceLoader;

    private byte[] logo;;
    
    @Autowired
    public CkanDepositServiceImpl(CkanServiceProperties ckanServiceProperties, CkanBuilder mapper, FileStorageService storageService, ResourceLoader resourceLoader){
        this.ckanServiceProperties = ckanServiceProperties;
        this.ckanBuilder = mapper;
	    this.storageService = storageService;
        this.resourceLoader = resourceLoader;
        this.logo = null;
    }

    @Override
    public String deposit(PlanDepositModel planDepositModel) {

        DepositConfiguration depositConfiguration = this.getConfiguration();

        if(depositConfiguration != null && planDepositModel != null && planDepositModel.getPlanModel() != null) {

            String token = null;
            if (planDepositModel.getAuthInfo() != null) {
                if (planDepositModel.getAuthInfo().getAuthToken() != null && !planDepositModel.getAuthInfo().getAuthToken().isBlank()) token = planDepositModel.getAuthInfo().getAuthToken();
                else if (planDepositModel.getAuthInfo().getAuthFields() != null && !planDepositModel.getAuthInfo().getAuthFields().isEmpty() && depositConfiguration.getUserConfigurationFields() != null) {
                    PluginUserFieldModel userFieldModel = planDepositModel.getAuthInfo().getAuthFields().stream().filter(x -> x.getCode().equals(CONFIGURATION_FIELD_ACCESS_TOKEN)).findFirst().orElse(null);
                    if (userFieldModel != null && userFieldModel.getTextValue() != null && !userFieldModel.getTextValue().isBlank()) token = userFieldModel.getTextValue();
                }
            }

            if (token == null || token.isBlank()) token = this.ckanServiceProperties.getDepositConfiguration().getAccessToken();
            String previousDOI = planDepositModel.getPlanModel().getPreviousDOI();

            try {

                if (previousDOI == null) {
                    return depositFirst(planDepositModel.getPlanModel(), token);
                } else {
                    return depositNewVersion(planDepositModel.getPlanModel(), token);
                }

            } catch (HttpClientErrorException | HttpServerErrorException ex) {
                logger.error(ex.getMessage(), ex);
                Map<String, String> parsedException = objectMapper.readValue(ex.getResponseBodyAsString(), Map.class);
                try {
                    throw new IOException(parsedException.get("message"), ex);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        }

        return null;

    }


    private String depositFirst(PlanModel planModel, String token)  {
        CkanDataset dataset = this.ckanBuilder.build(planModel);

        String url = this.ckanServiceProperties.getDepositConfiguration().getRepositoryUrl() + "package_create";

        Map<String, Object> response = this.getWebClient().post().uri(url).headers(httpHeaders -> {
                    httpHeaders.set("Authorization", token);
                    httpHeaders.setContentType(MediaType.APPLICATION_JSON);
                })
                .bodyValue(dataset).exchangeToMono(mono ->
                        mono.statusCode().isError() ?
                                mono.createException().flatMap(Mono::error) :
                                mono.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})).block();

        response = (Map<String, Object>) response.get("result");
        String id = String.valueOf(response.get("id"));
        this.uploadFiles(planModel, id, token);
        this.sendRelationshipFromSemantics(planModel, id, token);
        return String.valueOf(response.get("name"));
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

        CkanUploadData data = new CkanUploadData();
        data.setDatasetId(datasetId);
        data.setName(fileEnvelopeModel.getFilename());

        this.getWebClient().post().uri(url).headers(httpHeaders -> {
                    httpHeaders.set("Authorization", token);
                    httpHeaders.setContentType(MediaType.APPLICATION_JSON);
                })
                .bodyValue(data).exchangeToMono(mono ->
                        mono.statusCode().isError() ?
                                mono.createException().flatMap(Mono::error) :
                                mono.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})).block();
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

    private String depositNewVersion(PlanModel planModel, String token) {
        JsonNode oldDatasetJson = this.getDatasetIdentifier(planModel.getPreviousDOI()).get(0);
        String oldDatasetId = oldDatasetJson.get("id").asText();
        String oldVersion = oldDatasetJson.get("version").asText();

        Map<String, Object> newDataset = this.updateVersion(planModel, oldVersion, token);

        if (newDataset != null && newDataset.get("id") != null) {
            this.uploadFiles(planModel, newDataset.get("id").toString(), token);
            this.createNewVersionRelationship(oldDatasetId, newDataset.get("id").toString(), token);
            this.sendRelationshipFromSemantics(planModel, newDataset.get("id").toString(), token);
            return (String) newDataset.get("name");
        }
        return null;
    }

    private JsonNode getDatasetIdentifier(String previousDOI) throws JacksonException {
        String url = this.ckanServiceProperties.getDepositConfiguration().getRepositoryUrl() + "package_search?q=name:" + previousDOI + "&include_private=True";

        Map<String, Object> response = this.getWebClient().get().uri(url).retrieve().toEntity(Map.class).block().getBody();
        if (response == null) return null;

        JsonNode jsonNode = objectMapper.readTree(new JSONObject(response).toString()).get("result");
        return jsonNode.findValues("results").getFirst();
    }

    private Map<String, Object> updateVersion(PlanModel planModel, String version, String token){

        CkanDataset dataset = this.ckanBuilder.build(planModel);
        dataset.setVersion(String.valueOf(Integer.parseInt(version) + 1));

        String url = this.ckanServiceProperties.getDepositConfiguration().getRepositoryUrl() + "package_create";

        Map<String, Object> response = this.getWebClient().post().uri(url).headers(httpHeaders -> {
                    httpHeaders.set("Authorization", token);
                    httpHeaders.setContentType(MediaType.APPLICATION_JSON);
                })
                .bodyValue(dataset).exchangeToMono(mono ->
                        mono.statusCode().isError() ?
                                mono.createException().flatMap(Mono::error) :
                                mono.bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})).block();
        if (response == null) return null;
        return (Map<String, Object>) response.get("result");
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
                return response.mutate().build().bodyToMono(String.class)
                    .flatMap(body -> {
                        logger.error(new MapLogEntry("Response").And("method", response.request().getMethod().toString()).And("url", response.request().getURI()).And("status", response.statusCode().toString()).And("body", body));
                        return Mono.just(response);
                    });
            }
            return Mono.just(response);
            
        });
    }
}
