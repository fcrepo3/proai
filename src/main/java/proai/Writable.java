package proai;

import proai.error.ServerException;

import java.io.PrintWriter;

public interface Writable {

    public void write(PrintWriter out) throws ServerException;

}