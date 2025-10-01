package org.opencdmp.deposit.ckan.service.ckan;

import org.opencdmp.depositbase.repository.DepositConfiguration;
import org.opencdmp.depositbase.repository.PlanDepositModel;

public interface CkanDepositService {
	String deposit(PlanDepositModel planDepositModel) throws Exception;

	DepositConfiguration getConfiguration();

	String authenticate(String code);

	String getLogo();
}
