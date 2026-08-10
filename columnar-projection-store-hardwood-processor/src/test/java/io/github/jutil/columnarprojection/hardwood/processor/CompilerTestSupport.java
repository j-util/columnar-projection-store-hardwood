package io.github.jutil.columnarprojection.hardwood.processor;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.processing.Processor;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

final class CompilerTestSupport {

    private CompilerTestSupport() {
    }

    static Compilation compile(
            Path root,
            Map<String, String> sources,
            List<? extends Processor> processors) throws IOException {
        Path sourceDirectory = Files.createDirectories(root.resolve("sources"));
        Path classesDirectory = Files.createDirectories(root.resolve("classes"));
        Path generatedDirectory = Files.createDirectories(root.resolve("generated"));
        List<Path> sourceFiles = new ArrayList<>();
        for (Map.Entry<String, String> source : sources.entrySet()) {
            Path sourceFile = sourceDirectory.resolve(
                    source.getKey().replace('.', '/') + ".java");
            Files.createDirectories(sourceFile.getParent());
            Files.writeString(sourceFile, source.getValue());
            sourceFiles.add(sourceFile);
        }

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new IllegalStateException("Tests require a full JDK");
        }
        DiagnosticCollector<JavaFileObject> diagnostics =
                new DiagnosticCollector<>();
        try (StandardJavaFileManager fileManager = compiler
                .getStandardFileManager(diagnostics, null, null)) {
            Iterable<? extends JavaFileObject> compilationUnits = fileManager
                    .getJavaFileObjectsFromPaths(sourceFiles);
            List<String> options = List.of(
                    "--release", "21",
                    "-proc:full",
                    "-classpath", testClasspath(),
                    "-d", classesDirectory.toString(),
                    "-s", generatedDirectory.toString());
            JavaCompiler.CompilationTask task = compiler.getTask(
                    null,
                    fileManager,
                    diagnostics,
                    options,
                    null,
                    compilationUnits);
            task.setProcessors(processors);
            boolean successful = Boolean.TRUE.equals(task.call());
            return new Compilation(
                    successful,
                    List.copyOf(diagnostics.getDiagnostics()),
                    classesDirectory,
                    generatedDirectory);
        }
    }

    private static String testClasspath() {
        String surefireClasspath = System.getProperty("surefire.test.class.path");
        return surefireClasspath == null
                ? System.getProperty("java.class.path")
                : surefireClasspath;
    }

    record Compilation(
            boolean successful,
            List<Diagnostic<? extends JavaFileObject>> diagnostics,
            Path classesDirectory,
            Path generatedDirectory) {

        String messages() {
            return diagnostics.stream()
                    .map(diagnostic -> diagnostic.getKind() + ": "
                            + diagnostic.getMessage(null))
                    .collect(Collectors.joining(System.lineSeparator()));
        }

        String generatedSource(String qualifiedName) throws IOException {
            return Files.readString(generatedDirectory.resolve(
                    qualifiedName.replace('.', '/') + ".java"));
        }

        URLClassLoader classLoader() throws IOException {
            URL classes = classesDirectory.toUri().toURL();
            return new URLClassLoader(
                    new URL[]{classes},
                    Thread.currentThread().getContextClassLoader());
        }
    }
}
