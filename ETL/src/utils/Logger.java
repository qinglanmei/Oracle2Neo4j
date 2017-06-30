package utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Date;

public class Logger {

	private String fileName;

	public Logger(String fileName) {
		this.fileName = fileName;
	}

	public void write(String logInfo) {
		File fileParentDir = new File("./log");
		if (!fileParentDir.exists())
			fileParentDir.mkdir();
		try(PrintWriter printWriter = new PrintWriter(new FileOutputStream("./log" + this.fileName, true))) {
			printWriter.println("[" + new Date() + "]" + logInfo);
			printWriter.flush();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

}
