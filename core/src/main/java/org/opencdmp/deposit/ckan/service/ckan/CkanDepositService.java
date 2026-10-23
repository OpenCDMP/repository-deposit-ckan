package org.opencdmp.deposit.ckan.service.ckan;

import org.opencdmp.commonmodels.models.deposit.DepositPreviewModel;
import org.opencdmp.commonmodels.models.deposit.PreviewFieldOption;
import org.opencdmp.depositbase.repository.DepositAccount;
import org.opencdmp.depositbase.repository.DepositConfiguration;
import org.opencdmp.depositbase.repository.PlanDepositModel;

import java.util.List;

public interface CkanDepositService {
	String deposit(PlanDepositModel planDepositModel) ;

	DepositPreviewModel preview(PlanDepositModel planDepositModel);

	List<PreviewFieldOption> searchVocabulary(String fieldCode, String like);

	DepositConfiguration getConfiguration();

	DepositAccount account(PlanDepositModel planDepositModel);

	String authenticate(String code);

	String getLogo();
}
