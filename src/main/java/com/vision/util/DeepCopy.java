package com.vision.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DeepCopy<E> {
	public List<E> copyCollection(List<E> orig){
		ArrayList<E> listVbObjects = new ArrayList<E>();
        for (Iterator<E> iterator = orig.iterator(); iterator.hasNext();) {
        	E obj = (E) iterator.next();
        	E objNew = copy(obj);
        	if(objNew != null)
        		listVbObjects.add(objNew);
         }
       return listVbObjects; 
	}
    public E copy(E orig) {
    	E obj = null;
    	try {
            // Write the object out to a byte array
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(orig);
            out.flush();
            out.close();
            // Make an input stream from the byte array and read
            // a copy of the object back in.
            ObjectInputStream in = new ObjectInputStream(
                new ByteArrayInputStream(bos.toByteArray()));
            obj = (E) in.readObject();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        catch(ClassNotFoundException cnfe) {
            cnfe.printStackTrace();
        }
        return obj;
    }
}