package org.vanilladb.core.sql;

public class Test {

	public static void main(String[] args) {
		long a = new VarcharType().maxSize();
		a += 10;
		if (a > Integer.MAX_VALUE)
			System.out.println("haha");
		System.out.println("" + a);
	}

}
