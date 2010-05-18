/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.query.processor.relational;

import static org.teiid.query.analysis.AnalysisRecord.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.teiid.client.plan.PlanNode;
import org.teiid.common.buffer.BlockedException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.processor.ProcessorDataManager;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.util.CommandContext;


public class SelectNode extends SubqueryAwareRelationalNode {

	private Criteria criteria;
    
    // Derived element lookup map
    private Map elementMap; 
    private int[] projectionIndexes;
	
    // State if blocked on evaluating a criteria
    private boolean blockedOnCriteria = false;
    private boolean blockedOnPrepare = false;
    private TupleBatch blockedBatch = null;
    private int blockedRow = 0;
    
	public SelectNode(int nodeID) {
		super(nodeID);
	}
	
    public void reset() {
        super.reset();
        
        blockedOnCriteria = false;
        blockedOnPrepare = false;
        blockedBatch = null;
        blockedRow = 0;
    }

	public void setCriteria(Criteria criteria) { 
		this.criteria = criteria;
	}

	public Criteria getCriteria() { // made public to support change in ProcedurePlanner
		return this.criteria;
	}
	
	@Override
	public void initialize(CommandContext context, BufferManager bufferManager,
			ProcessorDataManager dataMgr) {
		super.initialize(context, bufferManager, dataMgr);
        // Create element lookup map for evaluating project expressions
        if(this.elementMap == null) {
            this.elementMap = createLookupMap(this.getChildren()[0].getElements());
            this.projectionIndexes = getProjectionIndexes(this.elementMap, getElements());
        }
	}
	
    /**
     * @see org.teiid.query.processor.relational.RelationalNode#nextBatchDirect()
     */
	public TupleBatch nextBatchDirect()
		throws BlockedException, TeiidComponentException, TeiidProcessingException {

        TupleBatch batch = blockedBatch; 		
        if(! blockedOnCriteria && ! blockedOnPrepare) {	
            batch = this.getChildren()[0].nextBatch();
        }
                
        int row = blockedRow;
        if(! blockedOnCriteria && ! blockedOnPrepare) {
            row = batch.getBeginRow();               
        } else {
            // Reset blocked state
            blockedOnCriteria = false;
            blockedOnPrepare = false;
            blockedBatch = null;
            blockedRow = 0;
        }
        
        for(; row <= batch.getEndRow(); row++) {             
            List tuple = batch.getTuple(row);
        
            // Evaluate criteria with tuple
            try {
                if(getEvaluator(this.elementMap).evaluate(this.criteria, tuple)) {
                    addBatchRow(projectTuple(this.projectionIndexes, tuple));
                }
            } catch(BlockedException e) {
                // Save state and rethrow
                blockedOnCriteria = true;
                blockedBatch = batch;
                blockedRow = row;
                throw e;   
            }
        }   

        if(batch.getTerminationFlag()) { 
            terminateBatches();
        }

        return pullBatch();            
	}
    
	protected void getNodeString(StringBuffer str) {
		super.getNodeString(str);
		str.append(criteria);
	}
	
	public Object clone(){
		SelectNode clonedNode = new SelectNode(super.getID());
		this.copy(this, clonedNode);
		return clonedNode;
	}
	
	protected void copy(SelectNode source, SelectNode target){
		super.copy(source, target);
		target.criteria = criteria;
		target.elementMap = source.elementMap;
		target.projectionIndexes = source.projectionIndexes;
	}
    
    /* 
     * @see com.metamatrix.query.processor.Describable#getDescriptionProperties()
     */
    public PlanNode getDescriptionProperties() {   
    	PlanNode props = super.getDescriptionProperties();
    	AnalysisRecord.addLanaguageObjects(props, PROP_CRITERIA, Arrays.asList(this.criteria));
        return props;
    }
    
}