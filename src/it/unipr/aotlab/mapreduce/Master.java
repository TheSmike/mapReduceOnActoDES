package it.unipr.aotlab.mapreduce;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

import it.unipr.aotlab.actodes.actor.Behavior;
import it.unipr.aotlab.actodes.actor.Binder;
import it.unipr.aotlab.actodes.actor.Case;
import it.unipr.aotlab.actodes.actor.Reference;
import it.unipr.aotlab.actodes.configuration.Configuration;
import it.unipr.aotlab.actodes.examples.buffer.EmptyBuffer;
import it.unipr.aotlab.actodes.interaction.Kill;
import it.unipr.aotlab.actodes.logging.ConsoleWriter;
import it.unipr.aotlab.actodes.logging.Logger;
import it.unipr.aotlab.actodes.logging.TextualFormatter;
import it.unipr.aotlab.actodes.runtime.Controller;
import it.unipr.aotlab.actodes.runtime.Shutdown;
import it.unipr.aotlab.actodes.runtime.passive.OldScheduler;

/**
 *
 * The {@code Initiator1} class defines a behavior that creates an
 * {@code EmptyBuffer} actor and a set of (a {@code Producer} and a
 * {@code Consumer}) actors.
 *
 * After a fixed period of time it asks them to kill themselves.
 *
 * @see EmptyBuffer
 * @see Producer
 * @see Consumer
 *
 **/

public final class Master extends Behavior {
	private static final long serialVersionUID = 1L;

	private static final String FS = "La stringa %s ha %n caratteri!\n";
	private String stringa;
	private int conta_caratteri;
	// private int elements;
	// private int messages;
	// private int attempts;
	// private int sent;
	// private Reference[] references;
	// private Reference current;
	// private final Random random;
	private int n_strings;
	private int n_workers;
	private String[] array_di_stringhe;
	private Queue<String> stringhe;
	private final Random random;
	private int sent;
	private int attempts;
	private int totale;
	private int risposte;
	private Reference[] references;
	private Reference current;

	// Alive processing case.
	private Case process;

	/**
	 * Class constructor.
	 *
	 **/

	public Master() {

		this.n_strings = 0;
		this.n_workers = 0;
		this.array_di_stringhe = new String[n_strings];

		this.sent = 0;
		this.attempts = 0;
		this.conta_caratteri = 0;

		this.random = new Random();

		this.references = null;
		this.current = null;

	}

	@Override
	public void initialize(final Binder b, final Object[] v) {
		if (((int) v[0] > 0) && ((int) v[1] > 0)) {
			// HashSet<Reference> consumers = new HashSet<>();
			// HashSet<Reference> producers = new HashSet<>();
			this.n_strings = (int) v[1];
			this.n_workers = (int) v[0];

			// setTimeout((long) v[0]); //definisco un timeout attraverso al
			// behaviour associato all'attore

			// Reference buffer = actor(new EmptyBuffer(), (int) v[1]);
			HashSet<Reference> referenze = new HashSet<>();
			this.references = new Reference[this.n_workers];
			this.array_di_stringhe = new String[n_strings];
			this.stringhe = new LinkedList<>();

			for (int i = 0; i < this.n_workers; i++) {
				//referenze.add(actor(new Worker()));
				this.references[i] = actor(new Worker());
			}

			for (int i = 0; i < this.n_strings; i++) {
				String spazio = " ";

				for (int j = 0; j < i; j++) {

					spazio += spazio;

				}
				array_di_stringhe[i] = "ciao" + spazio;
				stringhe.add(array_di_stringhe[i]);

			}

			// this.stringa =
			// this.array_di_stringhe[this.random.nextInt(this.n_strings)];
			// this.conta_caratteri = this.stringa.length();

			this.current = this.references[this.random.nextInt(this.n_workers)];

			totale = 0;
			risposte = 0;

			this.process = (m) -> {
				
				System.out.println("risposta da " + m.getSender().getName() + ": " + m.getContent());
				totale += (int) m.getContent();
				risposte++;
				

				// future(this.current, message , this.process);
				if (!stringhe.isEmpty())
				{
					String messaggio = stringhe.remove();
					future(m,messaggio, this.process);
					
					
				}
				else if(stringhe.isEmpty() && (risposte == n_strings)  )
				{
					System.out.print("totale contatore stringhe: "+totale+"\n");
					

			        for (int i = 0; i < n_workers; i++)
			        {
			        	Reference kill_worker = this.current = this.references[i];
			        	send(kill_worker, Kill.KILL);
			        }

			        //send(buffer, Kill.KILL);

			        return Shutdown.INSTANCE;
				}
				
					
					

				return null;
			};

			int i;
			for (i = 0; i < Math.min(this.n_strings, this.n_workers); i++) {

				String message = this.stringa = array_di_stringhe[i];

				String messaggio = stringhe.remove();

				this.current = this.references[i];

				// future(this.current, message , this.process);
				future(this.current, messaggio, this.process);

			}

			/*
			 * this.process = (m) -> { this.sent++; if (this.sent <
			 * this.n_strings) { future(this.current, Alive.ALIVE,
			 * this.process); //this.conta_caratteri = this.stringa.length();
			 * //System.out.format( // FS, this.stringa, this.conta_caratteri);
			 * //System.out.println("ciao");
			 * 
			 * } else if (this.attempts < this.n_workers) { this.attempts++;
			 * 
			 * this.sent = 0;
			 * 
			 * this.current =
			 * this.references[this.random.nextInt(this.n_workers)];
			 * 
			 * future(this.current, Alive.ALIVE, this.process); } else {
			 * send(GBROADCAST, Kill.KILL);
			 * 
			 * return Shutdown.INSTANCE; }
			 * 
			 * return null; };
			 * 
			 * future(this.current, Alive.ALIVE, this.process);
			 * 
			 */

		}

	}

	/**
	 * Starts an actor space running the buffer example.
	 *
	 * @param v
	 *            the arguments.
	 *
	 *            It does not need arguments.
	 *
	 **/
	public static void main(final String[] v) {
		final int workers = 3;
		final int stringhe = 5;

		Configuration c = Controller.INSTANCE.getConfiguration();

		c.setScheduler(OldScheduler.class.getName());

		c.setCreator(Master.class.getName());
		c.setArguments(workers, stringhe);

		c.setFilter(Logger.ALLLOGS);

		c.addWriter(ConsoleWriter.class.getName(), TextualFormatter.class.getName(), null);

		Controller.INSTANCE.run();

	}
}
