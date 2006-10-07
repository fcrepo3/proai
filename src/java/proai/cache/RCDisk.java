package proai.cache;

import java.io.*;
import java.text.*;
import java.util.*;

import org.apache.log4j.Logger;

import proai.Writable;
import proai.error.ServerException;
import proai.util.StreamUtil;

/**
 * The file-based portion of the record cache.
 */
public class RCDisk {

    public static final String PATH_DATE_PATTERN = "yyyy/MM/dd/HH/mm/ss.SSS.'xml'";

    private static final Logger logger =
            Logger.getLogger(RCDisk.class.getName());

    private File m_baseDir;
    private long _lastPathDate;

    public RCDisk(File baseDir) {
        m_baseDir = baseDir;
        if (!m_baseDir.exists()) {
            m_baseDir.mkdirs();
        }
    }

    /**
     * Get a new, unique path (relative to m_baseDir) for a file, based on 
     * the current time.
     *
     * If the directory for the path does not yet exist, it will be created.
     */
    private String getNewPath() {
        DateFormat formatter = new SimpleDateFormat(PATH_DATE_PATTERN);
        String path = null;
        synchronized (this) {
            long now = System.currentTimeMillis();
            while (now == _lastPathDate) {
                logger.debug("Path date collision... waiting for a unique date");
                try { Thread.sleep(10); } catch (Exception e) { } // make sure we have a unique date
                now = System.currentTimeMillis();
            }
            path = formatter.format(new Date(now));
            _lastPathDate = now;
        }
        File dir = new File(m_baseDir, path.substring(0, 16));
        dir.mkdirs();
        return path;
    }

    /**
     * Get a new RCDiskWriter backed by a new file in the disk cache.
     */
    public RCDiskWriter getNewWriter() throws ServerException {
        String path = getNewPath();
        try {
            return new RCDiskWriter(m_baseDir, path);
        } catch (Exception e) {
            throw new ServerException("Error creating new cache file: " + path, e);
        }
    }

    /**
     * Write the content of the given <code>Writable</code> to a new file and 
     * return the path of the file, relative to the disk cache base directory.
     */
    public String write(Writable writable) throws ServerException {
        String path = getNewPath();
        try {
            PrintWriter writer = new PrintWriter(
                                     new OutputStreamWriter(
                                         new FileOutputStream(
                                             new File(m_baseDir, path)), 
                                             "UTF-8"));
            writable.write(writer);
            writer.close();
            return path;
        } catch (Exception e) {
            throw new ServerException("Error writing stream to file in cache: " + path, e);
        }
    }

    public File getFile(String path) {
        return new File(m_baseDir, path);
    }

    public CachedContent getContent(String path) {
        if (path == null) return null;
        return new CachedContent(getFile(path));
    }

    // Same as getContent, but re-writes the <datestamp> and optionally only returns the header
    public CachedContent getContent(String path, String dateStamp, boolean headerOnly) {
        return new CachedContent(getFile(path), dateStamp, headerOnly);
    }

    public void delete(String path) {
        new File(m_baseDir, path).delete();
    }

    /**
     * Deletes path and all files created after it while pruning any empty
     * directories that come about as a result.
     */
    public void cancel(String path) throws ServerException {
        int year;
        int month;
        int day;
        int hour;
        int minute;
        double second;
        String[] parts = path.split("/");
        try {
            year = Integer.parseInt(parts[0]);
            month = Integer.parseInt(parts[1]);
            day = Integer.parseInt(parts[2]);
            hour = Integer.parseInt(parts[3]);
            minute = Integer.parseInt(parts[4]);
            second = Double.parseDouble(parts[5].substring(0, 6));
        } catch (Exception e) {
            throw new ServerException("Error parsing cache file path: " + path, e);
        }
        File[] yearDirs = m_baseDir.listFiles();
        if (yearDirs != null) {
            for (int yearIndex = 0; yearIndex < yearDirs.length; yearIndex++) {
                try {
                    if (Integer.parseInt(yearDirs[yearIndex].getName()) >= year) {
                        File[] monthDirs = yearDirs[yearIndex].listFiles();
                        if (monthDirs != null) {
                            for (int monthIndex = 0; monthIndex < monthDirs.length; monthIndex++) {
                                try {
                                    if (Integer.parseInt(monthDirs[monthIndex].getName()) >= month) {
                                        File[] dayDirs = monthDirs[monthIndex].listFiles();
                                        if (dayDirs != null) {
                                            for (int dayIndex = 0; dayIndex < dayDirs.length; dayIndex++) {
                                                try {
                                                    if (Integer.parseInt(dayDirs[dayIndex].getName()) >= day) {
                                                        File[] hourDirs = dayDirs[dayIndex].listFiles();
                                                        if (hourDirs != null) {
                                                            for (int hourIndex = 0; hourIndex < hourDirs.length; hourIndex++) {
                                                                try {
                                                                    if (Integer.parseInt(hourDirs[hourIndex].getName()) >= hour) {
                                                                        File[] minuteDirs = hourDirs[hourIndex].listFiles();
                                                                        if (minuteDirs != null) {
                                                                            for (int minuteIndex = 0; minuteIndex < minuteDirs.length; minuteIndex++) {
                                                                                try {
                                                                                    int minNum = Integer.parseInt(minuteDirs[minuteIndex].getName());
                                                                                    if (minNum == minute) {
                                                                                        deleteIfNeeded(minuteDirs[minuteIndex], second);
                                                                                    } else if (minNum > minute) {
                                                                                        // if minNum > minute, we know we need to delete all files in dir
                                                                                        deleteIfNeeded(minuteDirs[minuteIndex], 0.0);
                                                                                    }
                                                                                } catch (NumberFormatException e) {
                                                                                    e.printStackTrace();
                                                                                }
                                                                                deleteIfEmpty(minuteDirs[minuteIndex]);
                                                                            }
                                                                        }
                                                                    }
                                                                } catch (NumberFormatException e) {
                                                                    e.printStackTrace();
                                                                }
                                                                deleteIfEmpty(hourDirs[hourIndex]);
                                                            }
                                                        }
                                                    }
                                                } catch (NumberFormatException e) {
                                                    e.printStackTrace();
                                                }
                                                deleteIfEmpty(dayDirs[dayIndex]);
                                            }
                                        }
                                    }
                                } catch (NumberFormatException e) {
                                    e.printStackTrace();
                                }
                                deleteIfEmpty(monthDirs[monthIndex]);
                            }
                        }
                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
                deleteIfEmpty(yearDirs[yearIndex]);
            }
        }
    }

    private void deleteIfNeeded(File dir, double second) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                File file = files[i];
                try {
                    double fileSecond = Double.parseDouble(file.getName().substring(0, 6));
                    if (fileSecond >= second) {
                        if (file.delete()) {
                            logger.info("Deleted canceled file: " + file.getPath());
                        } else {
                            logger.warn("Could not delete unused file: " + file.getPath());
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Unable to parse " + file.getName());
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * This should be pretty fast.  It relies on a specific number of 
     * directories and the fact that File.delete() won't delete a directory 
     * if it is non-empty.
     */
    public void pruneEmptyDirs() {
        File[] yearDirs = m_baseDir.listFiles();
        if (yearDirs != null) {
            for (int yearIndex = 0; yearIndex < yearDirs.length; yearIndex++) {
                File[] monthDirs = yearDirs[yearIndex].listFiles();
                if (monthDirs != null) {
                    for (int monthIndex = 0; monthIndex < monthDirs.length; monthIndex++) {
                        File[] dayDirs = monthDirs[monthIndex].listFiles();
                        if (dayDirs != null) {
                            for (int dayIndex = 0; dayIndex < dayDirs.length; dayIndex++) {
                                File[] hourDirs = dayDirs[dayIndex].listFiles();
                                if (hourDirs != null) {
                                    for (int hourIndex = 0; hourIndex < hourDirs.length; hourIndex++) {
                                        File[] minuteDirs = hourDirs[hourIndex].listFiles();
                                        if (minuteDirs != null) {
                                            for (int minuteIndex = 0; minuteIndex < minuteDirs.length; minuteIndex++) {
                                                deleteIfEmpty(minuteDirs[minuteIndex]);
                                            }
                                        }
                                        deleteIfEmpty(hourDirs[hourIndex]);
                                    }
                                }
                                deleteIfEmpty(dayDirs[dayIndex]);
                            }
                        }
                        deleteIfEmpty(monthDirs[monthIndex]);
                    }
                }
                deleteIfEmpty(yearDirs[yearIndex]);
            }
        }
    }

    /**
     * Delete the given directory if it's empty.
     */
    private void deleteIfEmpty(File dir) {
        if (dir.delete()) {
            logger.info("Pruned directory " + dir.getPath());
        }
    }

    public void writeAllPaths(PrintWriter writer) {
        File[] yearDirs = m_baseDir.listFiles();
        if (yearDirs != null) {
            for (int yearIndex = 0; yearIndex < yearDirs.length; yearIndex++) {
                File[] monthDirs = yearDirs[yearIndex].listFiles();
                if (monthDirs != null) {
                    for (int monthIndex = 0; monthIndex < monthDirs.length; monthIndex++) {
                        File[] dayDirs = monthDirs[monthIndex].listFiles();
                        if (dayDirs != null) {
                            for (int dayIndex = 0; dayIndex < dayDirs.length; dayIndex++) {
                                File[] hourDirs = dayDirs[dayIndex].listFiles();
                                if (hourDirs != null) {
                                    for (int hourIndex = 0; hourIndex < hourDirs.length; hourIndex++) {
                                        File[] minuteDirs = hourDirs[hourIndex].listFiles();
                                        if (minuteDirs != null) {
                                            for (int minuteIndex = 0; minuteIndex < minuteDirs.length; minuteIndex++) {
                                                String[] files = minuteDirs[minuteIndex].list();
                                                for (int i = 0; i < files.length; i++) {
                                                    String path = yearDirs[yearIndex].getName()
                                                           + "/" + monthDirs[monthIndex].getName()
                                                           + "/" + dayDirs[dayIndex].getName()
                                                           + "/" + hourDirs[hourIndex].getName()
                                                           + "/" + minuteDirs[minuteIndex].getName()
                                                           + "/" + files[i];
                                                    writer.println(path);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}