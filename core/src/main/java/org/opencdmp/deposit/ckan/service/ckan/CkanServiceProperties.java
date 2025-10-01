package org.opencdmp.deposit.ckan.service.ckan;

import org.opencdmp.depositbase.repository.DepositConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ckan")
public class CkanServiceProperties {

    private String logo;

    private String domain;

    private String organization;

    private DepositConfiguration depositConfiguration;

    private String licenseReferenceCode;

    private String organizationReferenceCode;

    private int maxInMemorySizeInBytes;

    public String getLogo() {
        return logo;
    }

    public void setLogo(String logo) {
        this.logo = logo;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }

    public DepositConfiguration getDepositConfiguration() {
        return depositConfiguration;
    }

    public void setDepositConfiguration(DepositConfiguration depositConfiguration) {
        this.depositConfiguration = depositConfiguration;
    }

    public String getLicenseReferenceCode() {
        return licenseReferenceCode;
    }

    public void setLicenseReferenceCode(String licenseReferenceCode) {
        this.licenseReferenceCode = licenseReferenceCode;
    }

    public String getOrganizationReferenceCode() {
        return organizationReferenceCode;
    }

    public void setOrganizationReferenceCode(String organizationReferenceCode) {
        this.organizationReferenceCode = organizationReferenceCode;
    }

    public int getMaxInMemorySizeInBytes() {
        return maxInMemorySizeInBytes;
    }

    public void setMaxInMemorySizeInBytes(int maxInMemorySizeInBytes) {
        this.maxInMemorySizeInBytes = maxInMemorySizeInBytes;
    }
}
