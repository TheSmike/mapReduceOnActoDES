package it.unipr.aotlab.mapreduce.countword;

import it.unipr.aotlab.actodes.actor.Behavior;
import it.unipr.aotlab.actodes.actor.Binder;

public final class WordCountMaster extends Behavior {

	@Override
	public void initialize(Binder b, Object[] v) {
		if (!checkInputValidity(v)){
			System.err.println("Fine programma");
			return; 
		}
	
		if (((int) v[0] > 0) && ((int) v[1] > 0)) {
			int n_workers = (int) v[0];
		}
	}

	private boolean checkInputValidity(Object[] v) {
		if (v.length != 4)
			throw new InitializeException();
			return false;
		
		return true;
	}

}
