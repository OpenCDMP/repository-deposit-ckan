package org.opencdmp.deposit.ckan.service.ckan;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({CkanServiceProperties.class})
public class CkanServiceConfiguration {
}
