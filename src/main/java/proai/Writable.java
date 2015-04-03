package proai;

import java.io.PrintWriter;

import proai.error.ServerException;

public interface Writable {

    public void write(PrintWriter out) throws ServerException;

}