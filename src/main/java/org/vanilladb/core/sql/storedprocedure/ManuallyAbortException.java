package org.vanilladb.core.sql.storedprocedure;

public class ManuallyAbortException extends RuntimeException {

	private static final long serialVersionUID = 20200211001L;
	
	public ManuallyAbortException() {
		super();
	}
	
	public ManuallyAbortException(String message) {
		super(message);
	}

}
