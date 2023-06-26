package com.vision.dao;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.vision.vb.CommonVb;

@Component
public abstract class AbstractDao<E extends CommonVb> extends AbstractTransactionalDao<E>{
	
	public List<E> getQueryResults(E dObj, int intStatus){
		return new ArrayList<E>();
	}
	public List<E> getQueryPopupResults(E dObj){
		return new ArrayList<E>();
	}
	public List<E> getQueryResultsForReview(E dObj, int status){
		return new ArrayList<E>();
	}
}