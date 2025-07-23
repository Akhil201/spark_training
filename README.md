To setup this project you need pre-requisites as follows, this document prepared that you are using mac keeping in mind

install
java - use jenv for managing java in macos
scala - use couriser for managing scala in macos
sbt - install using brew
spark


Refer this link for creating new sbt project
https://medium.com/@suffyan.asad1/spark-essentials-a-guide-to-setting-up-and-running-spark-projects-with-scala-and-sbt-80e2680d3528

Use coursier for managing scala versions and jenv for java in mac OS
https://www.mungingdata.com/java/jenv-multiple-versions-java/#:~:text=Here's%20the%20command%20to%20install,%2FJavaVirtualMachines%2Fadoptopenjdk%2D11.
regarding couriser, it is being updated in setup.txt file


If you have created project using Intellij or imported into intellij build your project with sbt in intellij
and your work will be good without compilation errors


run sbt command as follows

sbt "run com.spark.tutorials.Analysis data output"


We have used Template method design pattern for creating spark session and executing the business logic. 
If any new class is created take com.spark.tutorials.Analysis as reference and implement required methods from base class


===========================================================
Installing couriser in mac os
Detailed Steps:
1. Install Coursier:
   Open your terminal.
   Run the following command to install Coursier: brew install coursier/formulas/coursier && cs setup
   If you're on an Apple Silicon (M1, M2) Mac, or don't use Homebrew, use this alternative: curl -fL https://github.com/VirtusLab/coursier-m1/releases/latest/download/cs-aarch64-apple-darwin.gz | gzip -d > cs && chmod +x cs && (xattr -d com.apple.quarantine cs || true) && ./cs setup
   If you encounter errors or need to remove an older Coursier installation, consult the Scala documentation for troubleshooting steps.
2. Install Scala 2.13 using Coursier:
   In the terminal, use the command cs install scala:2.13.x (replace x with the desired minor version, e.g., 16). For example: cs install scala:2.13.16.
   You can also install the Scala compiler with cs install scalac:2.13.x.
3. Verify the installation:
   Check the installed Scala version using scala -version.
4. If you need to remove Scala 3.x (optional):
   If you have Scala 3 installed via Homebrew, you can remove it with brew uninstall scala (or brew uninstall scala@3 if you have multiple versions).
   If you installed Scala 3 using Coursier, you can remove it with cs uninstall scala:3.x.x.
5. Setting up SBT (if needed):
   If you use SBT (Scala Build Tool), you can specify the Scala version in your build.sbt file.
   For example, to use Scala 2.13.16, add the following to your build.sbt:










