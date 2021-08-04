package org.ipalich.mdm.process;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.compiere.model.MTable;
import org.compiere.model.PO;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;
import org.compiere.wf.DocWorkflowManager;
import org.compiere.wf.MWFNextCondition;
import org.compiere.wf.MWorkflow;

public class RunMdmChecks extends SvrProcess
{
	String	sqlStatement	= null;
	int		AD_Workflow_ID	= 0;

	@Override
	protected void prepare()
	{
		ProcessInfoParameter[] para = getParameter();

		for (int i = 0; i < para.length; i++)
		{
			String name = para[i].getParameterName();

			if (para[i].getParameter() == null)
			{
				continue;
			}
			else if (name.equals("SQLStatement"))
			{
				sqlStatement = para[i].getParameterAsString();
			}
			else if (name.equals("AD_Workflow_ID"))
			{
				AD_Workflow_ID = para[i].getParameterAsInt();
			}
		}
	}

	@Override
	protected String doIt() throws Exception
	{
		if (AD_Workflow_ID <= 0)
		{
			return "AD_Workflow_ID not found!";
		}

		MWorkflow wf = new MWorkflow(getCtx(), AD_Workflow_ID, get_TrxName());
		int AD_Table_ID_wf = wf.getAD_Table_ID();
		MTable table = new MTable(getCtx(), AD_Table_ID_wf, get_TrxName());
		
		int recordCount = 0;
		int workflowRunCount = 0;

		// in case of manual run from window, SQL statement would be empty
		if (sqlStatement == null || sqlStatement.equals(""))
		{
			sqlStatement = table.getTableName() + "_ID = " + getRecord_ID();
		}

		// find all records matching SQL statement
		int[] ids = MTable.getAllIDs(table.getTableName(), sqlStatement, get_TrxName());

		// check all Node Conditions without creating WF Activities
		for (int id : ids)
		{
			PO po = table.getPO(id, get_TrxName());

			// get all Node Conditions marked as MDM
			List<Integer> conditionIDs = getAllMdmConditionIDs();

			for (Integer condId : conditionIDs)
			{
				MWFNextCondition nc = new MWFNextCondition(getCtx(), condId, get_TrxName());

				// check for condition. If it's not satisfied - run WF to create alerts, etc.
				if (nc.evaluate(po))
				{
					DocWorkflowManager dwfm = DocWorkflowManager.get();
					dwfm.process(po, AD_Table_ID_wf);
					workflowRunCount++;

					break;
				}
			}
			
			recordCount++;
			
			commitEx();
		}

		String result = "Records processed: " + recordCount + ". WF runs: " + workflowRunCount + ".";

		return result;
	}

	/**
	 * 
	 * @return List of active Node Conditions marked as MDM from current WF
	 * @throws SQLException 
	 */
	private List<Integer> getAllMdmConditionIDs() throws SQLException
	{
		List<Integer> ids = new ArrayList<Integer>();

		String sql = ""
			+ "SELECT AD_WF_NextCondition_ID "
			+ "  FROM AD_WF_NextCondition nc "
			+ "  JOIN AD_WF_NodeNext nn ON (nn.AD_WF_NodeNext_ID = nc.AD_WF_NodeNext_ID) "
			+ "  JOIN AD_WF_Node n ON (n.AD_WF_Node_ID = nn.AD_WF_Node_ID) "
			+ " WHERE n.AD_Workflow_ID = ? " // 1
			+ "   AND nc.isMasterDataManagement = 'Y' "
			+ "   AND nc.IsActive = 'Y' "
			+ "   AND nn.IsActive = 'Y' "
			+ "   AND n.IsActive = 'Y' ";

		PreparedStatement pstmt = null;
		ResultSet rs = null;

		try
		{
			pstmt = DB.prepareStatement(sql, get_TrxName());
			pstmt.setInt(1, AD_Workflow_ID);
			rs = pstmt.executeQuery();

			while (rs.next())
			{
				ids.add(rs.getInt("AD_WF_NextCondition_ID"));
			}

		}
		catch (SQLException e)
		{
			throw e;
		}
		finally
		{
			DB.close(rs, pstmt);
		}

		return ids;
	}

}
