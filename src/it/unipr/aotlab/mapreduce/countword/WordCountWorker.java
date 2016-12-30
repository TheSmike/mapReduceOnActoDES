package it.unipr.aotlab.mapreduce.countword;

import it.unipr.aotlab.actodes.actor.Behavior;
import it.unipr.aotlab.actodes.actor.Binder;
import it.unipr.aotlab.actodes.actor.Case;
import it.unipr.aotlab.actodes.actor.KillableBehavior;
import it.unipr.aotlab.actodes.actor.Reference;
import it.unipr.aotlab.actodes.examples.buffer.Get;
import it.unipr.aotlab.actodes.examples.buffer.Put;
import it.unipr.aotlab.actodes.filtering.MessagePattern;
import it.unipr.aotlab.actodes.filtering.constraint.IsInstance;






public class WordCountWorker extends KillableBehavior {

	private static final long serialVersionUID = 1L;
	

	  private static final MessagePattern MAPPATTERN =
	      MessagePattern.contentPattern(new IsInstance("cosa che distingue un pattern map"));
	  private static final MessagePattern REDUCEPATTERN =
			  MessagePattern.contentPattern(new isInstance("cosa che distingue un pattern reduce"));

	
	//per avere la destinazione del master a cui comunicare il messaggio
	private Reference master_reference;
	  // Map function case.
	private Case mapCase;
	// Reduce function case.
	private Case reduceCase;
	
	


	@Override
	public void initialize(Binder b, Object[] v) {
		// TODO Auto-generated method stub
		super.initialize(b, v);
		
		
		//risposta nel caso di map_case
	    this.mapCase = (m) -> {
	    	future(this.master_reference, m , this.mapCase);
	    	//send(m, ((String) m.getContent()).length());
	    	//send(this.buffer, Get.GET);

	      return null;
	    };


	    this.reduceCase = (m) -> {
	    	future(this.master_reference, m , this.reduceCase);
	    

	      return null;
	    };
	    
	    future(this.master_reference, m , this.mapCase);
	    
	    future(this.master_reference, m , this.reduceCase);
	    

	    b.bind(KILLPATTERN, this.killCase);
	    b.bind(MAPPATTERN, this.mapCase);
	    b.bind(REDUCEPATTERN, this.reduceCase);
		
		
	}
}
