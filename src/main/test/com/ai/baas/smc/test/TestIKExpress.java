package com.ai.baas.smc.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.wltea.expression.ExpressionEvaluator;
import org.wltea.expression.datameta.Variable;

import com.ai.baas.storm.util.HBaseProxy;


public class TestIKExpress {

	
	
	public static void main (String [] args)
	{
		new TestIKExpress().shenpi();	
	}
	
	
//	public static void main(String [] args)
//	{
//
//		new TestIKExpress().shenpi();
//
//	}
	
	
	public void shenpi()
	{
		String c=">";
		String expression="a"+c+"b";
		 List<Variable> variables = new ArrayList<Variable>();
		    variables.add(Variable.createVariable("a", "3"));
		    variables.add(Variable.createVariable("b","4"));
		    Object result = ExpressionEvaluator.evaluate(expression, variables);
		    boolean flag=Boolean.parseBoolean(result.toString());
		    
//		IKin ikin=new IKin();
//		    
//		System.out.println(ikin.in("3", "{1,2,3,4}"));
		     
	}
		

	
}
