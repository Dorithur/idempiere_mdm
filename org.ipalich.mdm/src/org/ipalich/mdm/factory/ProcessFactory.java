package org.ipalich.mdm.factory;

import org.adempiere.base.IProcessFactory;
import org.compiere.process.ProcessCall;
import org.ipalich.mdm.process.RunMdmChecks;

public class ProcessFactory implements IProcessFactory
{

	@Override
	public ProcessCall newProcessInstance(String className)
	{
		if (className.equals("org.ipalich.mdm.process.RunMdmChecks"))
		{
			return new RunMdmChecks();
		}
		
		return null;
	}

}