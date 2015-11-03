package com.alexeygrigorev.codeforcescrawl;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

public class ProblemSubmissionUrlExtractor {

  public static void main(String[] args) throws Exception {
    WebDriver driver = new FirefoxDriver();

    PrintWriter pw = new PrintWriter(new File("data/problem-urls.txt"));
    for (int page = 1; page <= 25; page++) {
      String problemsUrl = "http://codeforces.com/problemset/page/" + page;
      driver.get(problemsUrl);
      Thread.sleep(2000);

      List<WebElement> elements = driver.findElements(By.cssSelector("a[title='Participants solved the problem']"));

      for (WebElement a : elements) {
        pw.println(a.getAttribute("href"));
        System.out.println(a.getAttribute("href"));
      }
    }

    pw.close();
    driver.quit();
  }

}
