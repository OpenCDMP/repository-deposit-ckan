package org.opencdmp.deposit.ckan.configuration.preview;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({PreviewExtraFieldsProperties.class})
public class PreviewConfiguration {
}
