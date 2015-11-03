package com.alexeygrigorev.codeforcescrawl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Point;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.Select;
import org.openqa.selenium.support.ui.WebDriverWait;

import com.google.common.collect.Queues;

public class CodeExtractor implements Runnable {

  private static String EXISTS_QUERY = "select count(1) from codeforces.submissions where submission_id=?;";
  private static String INSERT_QUERY = "INSERT INTO submissions (submission_id, source, status, "
      + "language, problem) VALUES (?,?,?,?,?)";

  private static Connection CONNECTION;

  private static Set<String> processedUrls;
  private static PrintWriter writerUrls;

  private final FirefoxDriver driver;
  private final WebDriverWait wait;
  private Queue<String> queue;

  public static void main(String[] args) throws Exception {
    Class.forName("com.mysql.jdbc.Driver");
    CONNECTION = DriverManager.getConnection("jdbc:mysql://localhost:3306/codeforces", "root", "");

    Queue<String> queue = Queues.newLinkedBlockingDeque();
    List<String> problemUrls = FileUtils.readLines(new File("data/problem-urls.txt"));
    queue.addAll(problemUrls);

    processedUrls = new HashSet<>(FileUtils.readLines(new File("data/processed-urls.txt")));
    writerUrls = new PrintWriter(new FileOutputStream(new File("data/processed-urls.txt"), true));

    ExecutorService executor = Executors.newFixedThreadPool(2);
    executor.submit(new CodeExtractor(queue));
    executor.submit(new CodeExtractor(queue));

    executor.shutdown();
    try {
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      System.out.println("oops");
    }

    CONNECTION.close();
    writerUrls.close();
  }

  public CodeExtractor(Queue<String> queue) {
    this.queue = queue;
    this.driver = new FirefoxDriver();
    this.wait = new WebDriverWait(driver, 5);
  }

  @Override
  public void run() {
    while (!queue.isEmpty()) {
      String url = queue.poll();
      try {
        processTask(url);
      } catch (Exception e) {
        e.printStackTrace();
        continue;
      }
    }

    driver.quit();
  }

  private void processTask(String url) throws Exception {
    System.out.println("processing " + url);
    if (processedUrls.contains(url)) {
      System.out.println("already processed " + url);
      return;
    }

    String startUrl = url + "/page/1?order=BY_ARRIVED_DESC";
    driver.get(startUrl);
    Thread.sleep(2000);

    WebElement form = driver.findElement(By.cssSelector("form.status-filter"));
    Select select = new Select(form.findElement(By.cssSelector("#verdictName")));
    select.selectByValue("anyVerdict");
    form.submit();
    Thread.sleep(2000);

    int max = -1;
    List<WebElement> pageIndexSpans = driver.findElementsByClassName("page-index");
    for (WebElement span : pageIndexSpans) {
      max = Math.max(max, Integer.parseInt(span.getAttribute("pageindex")));
    }

    System.out.println("max=" + max);

    for (int page = 1; page <= max; page++) {
      String pageUrl = url + "/page/" + page + "?order=BY_ARRIVED_DESC";
      System.out.println("processing " + pageUrl);
      if (processedUrls.contains(pageUrl)) {
        System.out.println("already processed " + pageUrl);
        continue;
      }

      try {
        processUrl(pageUrl);
        commit(pageUrl);
      } catch (Exception e) {
        e.printStackTrace();
        continue;
      }
    }

    commit(url);
  }

  private static synchronized void commit(String url) {
    processedUrls.add(url);
    writerUrls.println(url);
    writerUrls.flush();
  }

  private void processUrl(String url) throws Exception {
    driver.get(url);
    Thread.sleep(2000);

    List<WebElement> rows = driver.findElements(By.cssSelector("table.status-frame-datatable tr"));
    for (WebElement row : rows) {
      if ("first-row".equals(row.getAttribute("class"))) {
        continue;
      }

      try {
        processSubmission(row);
      } catch (Exception e) {
        e.printStackTrace();
        continue;
      }
    }
  }

  private void processSubmission(WebElement row) throws Exception {
    WebElement link = row.findElement(By.cssSelector("a.view-source"));
    int submissionId = Integer.parseInt(link.getText());

    if (alreadyParsed(submissionId)) {
      return;
    }

    System.out.println("opening " + submissionId);

    WebElement languageTd = row.findElement(By.xpath("td[5]"));
    String language = languageTd.getText();

    WebElement statusTd = row.findElement(By.xpath("td[6]"));
    String status = statusTd.getText();

    WebElement problemTd = row.findElement(By.xpath("td[4]"));
    String problem = problemTd.getText();

    scrollAndClick(driver, link);

    wait.until(ExpectedConditions.visibilityOfElementLocated(By.cssSelector("#facebox .close")));
    WebElement sourceElem = driver.findElement(By.xpath("//*[@id=\"facebox\"]/div/div/div/pre"));
    String source = sourceElem.getText();

    if (source.length() >= 20000) {
      return;
    }

    save(submissionId, language, status, problem, source);

    System.out.println("closing...");
    driver.findElement(By.cssSelector("#facebox .close")).click();
    Thread.sleep(100);
  }

  private static boolean alreadyParsed(int submissionId) throws SQLException {
    try (PreparedStatement statement = CONNECTION.prepareStatement(EXISTS_QUERY)) {
      statement.setInt(1, submissionId);
      try (ResultSet rs = statement.executeQuery()) {
        rs.next();
        int result = rs.getInt(1);
        return result == 1;
      }
    }
  }

  private static void save(int submissionId, String language, String status, String problem, String source)
      throws SQLException {
    try (PreparedStatement statement = CONNECTION.prepareStatement(INSERT_QUERY)) {
      statement.setInt(1, submissionId);
      statement.setString(2, source);
      statement.setString(3, status);
      statement.setString(4, language);
      statement.setString(5, problem);
      statement.execute();
    }
  }

  private static void scrollAndClick(FirefoxDriver driver, WebElement link) throws Exception {
    Point p = link.getLocation();
    ((JavascriptExecutor) driver).executeScript("window.scroll(" + p.getX() + "," + (p.getY() + 200) + ");");
    Thread.sleep(200);
    link.click();
  }

}
