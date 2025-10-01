package org.opencdmp.deposit.ckan.configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({SemanticsProperties.class})
public class SemanticsConfiguration {
}
