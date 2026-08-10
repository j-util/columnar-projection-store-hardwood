package io.github.jutil.columnarprojection.hardwood.processor;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.FilerException;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.ExecutableType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

/**
 * Generates direct Hardwood-to-store loaders for interfaces annotated with
 * {@code HardwoodProjection}.
 *
 * <p>The processor deliberately waits for Columnar Projection Store's 1.2.0
 * processor to generate its concrete implementation and public store and
 * batch contracts. It then inspects those contracts, so collision-safe names
 * and effective inherited or covariant accessors remain owned by the columnar
 * processor.
 */
@SupportedAnnotationTypes(
        "io.github.jutil.columnarprojection.hardwood.HardwoodProjection")
public final class HardwoodProjectionProcessor extends AbstractProcessor {

    private static final String HARDWOOD_PROJECTION =
            "io.github.jutil.columnarprojection.hardwood.HardwoodProjection";
    private static final String PROJECTION_SCHEMA =
            "io.github.jutil.columnarprojection.ProjectionSchema";
    private static final String PROJECTION_STORE =
            "io.github.jutil.columnarprojection.ProjectionStore";
    private static final String COLUMNAR_IMPLEMENTATION_SUFFIX =
            "__ColumnarProjectionStore";
    private static final String LOADER_SUFFIX = "HardwoodLoader";
    private static final String PROCESSOR_NAME =
            "io.github.jutil.columnarprojection.hardwood.processor."
                    + "HardwoodProjectionProcessor";

    private Elements elements;
    private Types types;
    private Filer filer;
    private Messager messager;
    private final Map<String, PendingSchema> pending = new LinkedHashMap<>();
    private final Map<String, TypeElement> loaderOwners =
            new LinkedHashMap<>();

    /**
     * Creates a processor for compiler service discovery.
     */
    public HardwoodProjectionProcessor() {
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void init(ProcessingEnvironment processingEnvironment) {
        super.init(processingEnvironment);
        elements = processingEnvironment.getElementUtils();
        types = processingEnvironment.getTypeUtils();
        filer = processingEnvironment.getFiler();
        messager = processingEnvironment.getMessager();
    }

    /**
     * Supports the latest source version understood by the compiler executing
     * this Java-21 processor.
     *
     * @return the latest supported source version
     */
    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    /** {@inheritDoc} */
    @Override
    public boolean process(
            Set<? extends TypeElement> annotations,
            RoundEnvironment roundEnvironment) {
        TypeElement hardwoodProjection =
                elements.getTypeElement(HARDWOOD_PROJECTION);
        if (hardwoodProjection != null) {
            for (Element element : roundEnvironment
                    .getElementsAnnotatedWith(hardwoodProjection)) {
                register(element);
            }
        }

        if (roundEnvironment.processingOver()) {
            reportMissingColumnarProcessor();
        } else {
            generateReadyLoaders();
        }
        return true;
    }

    private void register(Element element) {
        if (element.getKind() != ElementKind.INTERFACE) {
            error(element,
                    "@HardwoodProjection may only annotate an interface that "
                            + "is also annotated with @ProjectionSchema");
            return;
        }
        TypeElement schema = (TypeElement) element;
        if (!hasAnnotation(schema, PROJECTION_SCHEMA)) {
            error(schema,
                    "@HardwoodProjection requires the same interface to be "
                            + "annotated with @ProjectionSchema");
            return;
        }

        String binaryName = elements.getBinaryName(schema).toString();
        if (pending.containsKey(binaryName)) {
            return;
        }
        PackageElement packageElement = elements.getPackageOf(schema);
        String packageName = packageElement.getQualifiedName().toString();
        String binarySimpleName = packageName.isEmpty()
                ? binaryName
                : binaryName.substring(packageName.length() + 1);
        String loaderSimpleName = binarySimpleName + LOADER_SUFFIX;
        String loaderQualifiedName = qualify(packageName, loaderSimpleName);

        TypeElement priorOwner = loaderOwners.get(loaderQualifiedName);
        if (priorOwner != null && !priorOwner.equals(schema)) {
            error(schema,
                    "Generated Hardwood loader name collision: "
                            + loaderQualifiedName
                            + " is already reserved for projection schema "
                            + priorOwner.getQualifiedName());
            return;
        }
        TypeElement existingType = elements.getTypeElement(loaderQualifiedName);
        if (existingType != null) {
            error(schema,
                    "Generated Hardwood loader name collision: "
                            + loaderQualifiedName
                            + " is already declared by "
                            + existingType.getQualifiedName());
            return;
        }

        loaderOwners.put(loaderQualifiedName, schema);
        pending.put(binaryName, new PendingSchema(
                schema,
                binaryName,
                packageName,
                binarySimpleName,
                loaderSimpleName,
                loaderQualifiedName));
    }

    private void generateReadyLoaders() {
        List<String> completed = new ArrayList<>();
        for (Map.Entry<String, PendingSchema> entry : pending.entrySet()) {
            GenerationResult result = tryGenerate(entry.getValue());
            if (result != GenerationResult.WAITING) {
                completed.add(entry.getKey());
            }
        }
        for (String binaryName : completed) {
            pending.remove(binaryName);
        }
    }

    private GenerationResult tryGenerate(PendingSchema pendingSchema) {
        String implementationQualifiedName = qualify(
                pendingSchema.packageName,
                pendingSchema.binarySimpleName
                        + COLUMNAR_IMPLEMENTATION_SUFFIX);
        TypeElement implementation =
                elements.getTypeElement(implementationQualifiedName);
        if (implementation == null) {
            return GenerationResult.WAITING;
        }

        TypeElement projectionStore = elements.getTypeElement(PROJECTION_STORE);
        if (projectionStore == null) {
            error(pendingSchema.schema,
                    "Cannot inspect generated Columnar Projection Store "
                            + "contracts because " + PROJECTION_STORE
                            + " is not on the compilation class path");
            return GenerationResult.FAILED;
        }

        DeclaredType storeType = findStoreContract(
                pendingSchema.schema, implementation, projectionStore);
        if (storeType == null) {
            return GenerationResult.FAILED;
        }
        DeclaredType batchType = findRangedBatchType(
                pendingSchema.schema, storeType);
        if (batchType == null) {
            return GenerationResult.FAILED;
        }
        if (!hasSupportedConstructor(implementation)) {
            error(pendingSchema.schema,
                    "The generated concrete store "
                            + implementation.getQualifiedName()
                            + " does not expose its supported public "
                            + "constructor(int expectedSize)");
            return GenerationResult.FAILED;
        }

        List<ColumnBinding> columns = inspectBatchColumns(
                pendingSchema.schema, batchType);
        if (columns == null) {
            return GenerationResult.FAILED;
        }
        if (columns.isEmpty()) {
            error(pendingSchema.schema,
                    "The generated ranged batch contract has no column "
                            + "setters; a Hardwood projection must have at "
                            + "least one effective accessor");
            return GenerationResult.FAILED;
        }

        String source = generateSource(
                pendingSchema,
                implementation,
                storeType,
                batchType,
                columns);
        try {
            JavaFileObject sourceFile = filer.createSourceFile(
                    pendingSchema.loaderQualifiedName,
                    pendingSchema.schema);
            try (Writer writer = sourceFile.openWriter()) {
                writer.write(source);
            }
            return GenerationResult.GENERATED;
        } catch (FilerException exception) {
            error(pendingSchema.schema,
                    "Generated Hardwood loader name collision while creating "
                            + pendingSchema.loaderQualifiedName + ": "
                            + exception.getMessage());
        } catch (IOException exception) {
            error(pendingSchema.schema,
                    "Could not generate Hardwood loader "
                            + pendingSchema.loaderQualifiedName + ": "
                            + exception.getMessage());
        }
        return GenerationResult.FAILED;
    }

    private DeclaredType findStoreContract(
            TypeElement schema,
            TypeElement implementation,
            TypeElement projectionStore) {
        TypeMirror projectionStoreErasure =
                types.erasure(projectionStore.asType());
        DeclaredType found = null;
        for (TypeMirror interfaceType : implementation.getInterfaces()) {
            if (interfaceType.getKind() != TypeKind.DECLARED
                    || !types.isSubtype(
                            types.erasure(interfaceType),
                            projectionStoreErasure)) {
                continue;
            }
            if (found != null
                    && !types.isSameType(
                            types.erasure(found),
                            types.erasure(interfaceType))) {
                error(schema,
                        "Generated concrete store "
                                + implementation.getQualifiedName()
                                + " implements more than one projection-store "
                                + "contract; the Hardwood loader cannot choose "
                                + "one safely");
                return null;
            }
            found = (DeclaredType) interfaceType;
        }
        if (found == null) {
            error(schema,
                    "Generated concrete store "
                            + implementation.getQualifiedName()
                            + " does not implement a public schema-specific "
                            + "ProjectionStore contract");
        }
        return found;
    }

    private DeclaredType findRangedBatchType(
            TypeElement schema, DeclaredType storeType) {
        TypeElement storeElement = (TypeElement) storeType.asElement();
        DeclaredType found = null;
        for (ExecutableElement method : ElementFilter.methodsIn(
                elements.getAllMembers(storeElement))) {
            if (!method.getSimpleName().contentEquals("batch")
                    || method.getModifiers().contains(Modifier.STATIC)) {
                continue;
            }
            ExecutableType member = (ExecutableType) types.asMemberOf(
                    storeType, method);
            List<? extends TypeMirror> parameters =
                    member.getParameterTypes();
            if (parameters.size() != 2
                    || parameters.get(0).getKind() != TypeKind.INT
                    || parameters.get(1).getKind() != TypeKind.INT
                    || member.getReturnType().getKind()
                            != TypeKind.DECLARED) {
                continue;
            }
            DeclaredType candidate =
                    (DeclaredType) member.getReturnType();
            if (found == null
                    || types.isSubtype(
                            types.erasure(candidate),
                            types.erasure(found))) {
                found = candidate;
            }
        }
        if (found == null) {
            error(schema,
                    "Generated store contract " + storeElement.getQualifiedName()
                            + " has no ranged batch(int, int) method required "
                            + "for Hardwood arrays");
        }
        return found;
    }

    private boolean hasSupportedConstructor(TypeElement implementation) {
        for (ExecutableElement constructor : ElementFilter.constructorsIn(
                implementation.getEnclosedElements())) {
            if (constructor.getModifiers().contains(Modifier.PUBLIC)
                    && constructor.getParameters().size() == 1
                    && constructor.getParameters().get(0).asType().getKind()
                            == TypeKind.INT) {
                return true;
            }
        }
        return false;
    }

    private List<ColumnBinding> inspectBatchColumns(
            TypeElement schema, DeclaredType batchType) {
        TypeElement batchElement = (TypeElement) batchType.asElement();
        Map<String, ColumnBinding> byName = new LinkedHashMap<>();
        boolean valid = true;
        for (ExecutableElement method : ElementFilter.methodsIn(
                elements.getAllMembers(batchElement))) {
            if (method.getModifiers().contains(Modifier.STATIC)) {
                continue;
            }
            ExecutableType member = (ExecutableType) types.asMemberOf(
                    batchType, method);
            if (member.getParameterTypes().size() != 1
                    || member.getParameterTypes().get(0).getKind()
                            != TypeKind.ARRAY
                    || !types.isAssignable(
                            types.erasure(member.getReturnType()),
                            types.erasure(batchType))) {
                continue;
            }

            String name = method.getSimpleName().toString();
            TypeMirror parameterType = member.getParameterTypes().get(0);
            Mapping mapping = mappingFor((ArrayType) parameterType);
            if (mapping == null) {
                TypeMirror component =
                        ((ArrayType) parameterType).getComponentType();
                error(schema,
                        "Hardwood projection column '" + name
                                + "' has unsupported Java type " + component
                                + "; supported types are boolean, int, long, "
                                + "float, double, String, and byte[]");
                valid = false;
                continue;
            }

            ColumnBinding prior = byName.get(name);
            if (prior != null
                    && !types.isSameType(
                            types.erasure(prior.parameterType),
                            types.erasure(parameterType))) {
                error(schema,
                        "Generated batch contract contains conflicting "
                                + "effective setters for column '" + name
                                + "'; clean the compilation output and verify "
                                + "the projection's inherited accessors");
                valid = false;
                continue;
            }
            byName.putIfAbsent(name,
                    new ColumnBinding(name, parameterType, mapping));
        }
        if (!valid) {
            return null;
        }
        List<ColumnBinding> columns = new ArrayList<>(byName.values());
        columns.sort(Comparator.comparing(column -> column.name));
        return columns;
    }

    private Mapping mappingFor(ArrayType setterParameter) {
        TypeMirror component = setterParameter.getComponentType();
        return switch (component.getKind()) {
            case BOOLEAN -> new Mapping(
                    "getBooleans", true, "BOOLEAN", null);
            case INT -> new Mapping("getInts", true, "INT32", null);
            case LONG -> new Mapping("getLongs", true, "INT64", null);
            case FLOAT -> new Mapping("getFloats", true, "FLOAT", null);
            case DOUBLE -> new Mapping("getDoubles", true, "DOUBLE", null);
            case ARRAY -> isByteArray(component)
                    ? new Mapping(
                            "getBinaries",
                            false,
                            "BYTE_ARRAY",
                            "FIXED_LEN_BYTE_ARRAY")
                    : null;
            case DECLARED -> isJavaLangString(component)
                    ? new Mapping(
                            "getStrings",
                            false,
                            "BYTE_ARRAY",
                            "FIXED_LEN_BYTE_ARRAY")
                    : null;
            default -> null;
        };
    }

    private boolean isByteArray(TypeMirror type) {
        return type.getKind() == TypeKind.ARRAY
                && ((ArrayType) type).getComponentType().getKind()
                        == TypeKind.BYTE;
    }

    private boolean isJavaLangString(TypeMirror type) {
        TypeElement stringType = elements.getTypeElement("java.lang.String");
        return stringType != null
                && types.isSameType(
                        types.erasure(type),
                        types.erasure(stringType.asType()));
    }

    private String generateSource(
            PendingSchema pendingSchema,
            TypeElement implementation,
            DeclaredType storeType,
            DeclaredType batchType,
            List<ColumnBinding> columns) {
        String storeName = ((TypeElement) storeType.asElement())
                .getQualifiedName().toString();
        String implementationName = implementation.getQualifiedName().toString();
        String batchName = batchType.toString();
        StringBuilder source = new StringBuilder(16384);
        if (!pendingSchema.packageName.isEmpty()) {
            line(source, "package " + pendingSchema.packageName + ";");
            line(source, "");
        }
        line(source, "/**");
        line(source, " * Direct Hardwood column-batch loader for {@link "
                + pendingSchema.schema.getQualifiedName() + "}.");
        line(source, " *");
        line(source, " * <p>The loader is stateless. It validates the complete "
                + "flat column mapping before advancing input, copies aligned "
                + "batch ranges into a generated store, and seals the store "
                + "after successful exhaustion. Hardwood's column API is "
                + "experimental, so this generated integration is tied to "
                + "Hardwood 1.0.0.Final.");
        line(source, " */");
        line(source, "@javax.annotation.processing.Generated(\""
                + PROCESSOR_NAME + "\")");
        line(source, "public final class "
                + pendingSchema.loaderSimpleName + " {");
        line(source, "");
        line(source, "    private " + pendingSchema.loaderSimpleName
                + "() {");
        line(source, "    }");
        line(source, "");
        appendProjectionMethod(source, columns);
        appendReaderLoadMethod(source, storeName);
        appendAdvancedLoadMethod(
                source,
                storeName,
                implementationName,
                batchName,
                columns);
        appendValidationHelpers(source);
        line(source, "}");
        return source.toString();
    }

    private void appendProjectionMethod(
            StringBuilder source, List<ColumnBinding> columns) {
        line(source, "    /**");
        line(source, "     * Returns the Hardwood projection required by this "
                + "loader.");
        line(source, "     *");
        line(source, "     * @return a projection containing every effective "
                + "accessor name");
        line(source, "     */");
        line(source, "    public static dev.hardwood.schema.ColumnProjection "
                + "projection() {");
        line(source, "        return dev.hardwood.schema.ColumnProjection."
                + "columns(");
        for (int index = 0; index < columns.size(); index++) {
            String suffix = index + 1 == columns.size() ? "" : ",";
            line(source, "                \""
                    + escape(columns.get(index).name) + "\"" + suffix);
        }
        line(source, "        );");
        line(source, "    }");
        line(source, "");
    }

    private void appendReaderLoadMethod(
            StringBuilder source, String storeName) {
        line(source, "    /**");
        line(source, "     * Reads and materializes the requested columns from "
                + "a Parquet reader.");
        line(source, "     *");
        line(source, "     * <p>This method closes only the projected {@code "
                + "ColumnReaders} it creates. It never closes {@code reader}. "
                + "A single-file row count that fits in an {@code int} is "
                + "used only as an initial-capacity hint; multi-file and very "
                + "large inputs use zero.");
        line(source, "     *");
        line(source, "     * @param reader the caller-owned Parquet reader");
        line(source, "     * @return a sealed generated store");
        line(source, "     * @throws java.lang.NullPointerException if {@code "
                + "reader} is null");
        line(source, "     * @throws java.lang.IllegalArgumentException if the "
                + "Hardwood schema does not match this projection");
        line(source, "     */");
        line(source, "    public static " + storeName
                + " load(dev.hardwood.reader.ParquetFileReader reader) {");
        line(source, "        java.util.Objects.requireNonNull(reader, "
                + "\"reader\");");
        line(source, "        int expectedSize = 0;");
        line(source, "        if (!reader.isMultiFile()) {");
        line(source, "            long rowCount = reader.getFileMetaData()."
                + "numRows();");
        line(source, "            if (rowCount >= 0");
        line(source, "                    && rowCount <= java.lang.Integer."
                + "MAX_VALUE) {");
        line(source, "                expectedSize = (int) rowCount;");
        line(source, "            }");
        line(source, "        }");
        line(source, "        try (dev.hardwood.reader.ColumnReaders columns "
                + "= reader.buildColumnReaders(projection()).build()) {");
        line(source, "            return load(columns, expectedSize);");
        line(source, "        }");
        line(source, "    }");
        line(source, "");
    }

    private void appendAdvancedLoadMethod(
            StringBuilder source,
            String storeName,
            String implementationName,
            String batchName,
            List<ColumnBinding> columns) {
        line(source, "    /**");
        line(source, "     * Consumes caller-configured aligned column readers "
                + "to exhaustion.");
        line(source, "     *");
        line(source, "     * <p>The caller retains ownership of {@code "
                + "readers}; this method does not close it. Every mapping is "
                + "validated before the first batch is advanced. If reading "
                + "fails, no partial store is returned, although the input "
                + "may already have been partially consumed.");
        line(source, "     *");
        line(source, "     * @param readers caller-owned projected column "
                + "readers");
        line(source, "     * @param expectedSize initial-capacity hint, or zero "
                + "when unknown");
        line(source, "     * @return a sealed generated store");
        line(source, "     * @throws java.lang.NullPointerException if {@code "
                + "readers} is null");
        line(source, "     * @throws java.lang.IllegalArgumentException if "
                + "{@code expectedSize} is negative or a column mapping is "
                + "invalid");
        line(source, "     */");
        line(source, "    public static " + storeName
                + " load(dev.hardwood.reader.ColumnReaders readers, "
                + "int expectedSize) {");
        line(source, "        java.util.Objects.requireNonNull(readers, "
                + "\"readers\");");
        line(source, "        if (expectedSize < 0) {");
        line(source, "            throw new java.lang.IllegalArgumentException(" 
                + "\"expectedSize must be greater than or equal to zero: \" "
                + "+ expectedSize);");
        line(source, "        }");
        for (int index = 0; index < columns.size(); index++) {
            ColumnBinding column = columns.get(index);
            line(source, "        dev.hardwood.reader.ColumnReader column"
                    + index + " = requireColumn(readers, \""
                    + escape(column.name) + "\");");
            String secondPhysicalType = column.mapping.secondPhysicalType == null
                    ? "null"
                    : "dev.hardwood.metadata.PhysicalType."
                            + column.mapping.secondPhysicalType;
            line(source, "        validateColumn(column" + index + ", \""
                    + escape(column.name) + "\", "
                    + "dev.hardwood.metadata.PhysicalType."
                    + column.mapping.firstPhysicalType + ", "
                    + secondPhysicalType + ", "
                    + column.mapping.primitive + ");");
        }
        line(source, "");
        line(source, "        " + storeName + " store = new "
                + implementationName + "(expectedSize);");
        line(source, "        while (readers.nextBatch()) {");
        line(source, "            int recordCount = readers.getRecordCount();");
        line(source, "            " + batchName
                + " batch = store.batch(0, recordCount);");
        for (int index = 0; index < columns.size(); index++) {
            ColumnBinding column = columns.get(index);
            line(source, "            batch." + column.name + "(column"
                    + index + "." + column.mapping.readerAccessor + "());");
        }
        line(source, "            batch.append();");
        line(source, "        }");
        line(source, "        store.seal();");
        line(source, "        return store;");
        line(source, "    }");
        line(source, "");
    }

    private void appendValidationHelpers(StringBuilder source) {
        line(source, "    private static dev.hardwood.reader.ColumnReader "
                + "requireColumn(dev.hardwood.reader.ColumnReaders readers, "
                + "String name) {");
        line(source, "        try {");
        line(source, "            return readers.getColumnReader(name);");
        line(source, "        } catch (java.lang.IllegalArgumentException "
                + "missing) {");
        line(source, "            for (int index = 0; index < readers."
                + "getColumnCount(); index++) {");
        line(source, "                dev.hardwood.schema.ColumnSchema schema "
                + "= readers.getColumnReader(index).getColumnSchema();");
        line(source, "                if (schema.fieldPath().topLevelName()."
                + "equals(name)");
        line(source, "                        && schema.fieldPath().elements()."
                + "size() != 1) {");
        line(source, "                    throw new java.lang."
                + "IllegalArgumentException(");
        line(source, "                            \"Hardwood column '\" + name");
        line(source, "                                    + \"' is nested "
                + "(path \" + schema.fieldPath()");
        line(source, "                                    + \" has more than "
                + "one component); nested columns are not supported\", "
                + "missing);");
        line(source, "                }");
        line(source, "            }");
        line(source, "            throw new java.lang.IllegalArgumentException(" 
                + "\"Required Hardwood column '\" + name + \"' is missing\", "
                + "missing);");
        line(source, "        }");
        line(source, "    }");
        line(source, "");
        line(source, "    private static void validateColumn(");
        line(source, "            dev.hardwood.reader.ColumnReader reader,");
        line(source, "            String name,");
        line(source, "            dev.hardwood.metadata.PhysicalType first,");
        line(source, "            dev.hardwood.metadata.PhysicalType second,");
        line(source, "            boolean primitive) {");
        line(source, "        dev.hardwood.schema.ColumnSchema schema = "
                + "reader.getColumnSchema();");
        line(source, "        if (schema.fieldPath().elements().size() != 1 "
                + "|| schema.maxRepetitionLevel() != 0");
        line(source, "                || schema.repetitionType() == "
                + "dev.hardwood.metadata.RepetitionType.REPEATED) {");
        line(source, "            throw new java.lang.IllegalArgumentException(" 
                + "\"Hardwood column '\" + name + \"' is nested or repeated "
                + "(path \" + schema.fieldPath() + \"); only flat, "
                + "non-repeated columns are supported\");");
        line(source, "        }");
        line(source, "        if (primitive && schema.repetitionType() != "
                + "dev.hardwood.metadata.RepetitionType.REQUIRED) {");
        line(source, "            throw new java.lang.IllegalArgumentException(" 
                + "\"Primitive projection column '\" + name + \"' requires "
                + "a REQUIRED Parquet column, but it is \" + "
                + "schema.repetitionType());");
        line(source, "        }");
        line(source, "        if (schema.type() != first");
        line(source, "                && (second == null || schema.type() != "
                + "second)) {");
        line(source, "            String expected = second == null");
        line(source, "                    ? first.toString()");
        line(source, "                    : first + \" or \" + second;");
        line(source, "            throw new java.lang.IllegalArgumentException(" 
                + "\"Hardwood column '\" + name + \"' has physical type \" "
                + "+ schema.type() + \"; expected \" + expected);");
        line(source, "        }");
        line(source, "    }");
    }

    private void reportMissingColumnarProcessor() {
        for (PendingSchema pendingSchema : pending.values()) {
            error(pendingSchema.schema,
                    "Columnar Projection Store's annotation processor did "
                            + "not generate the store contract required for "
                            + pendingSchema.schema.getQualifiedName()
                            + ". Add io.github.j-util:"
                            + "columnar-projection-store-processor:1.2.0 "
                            + "alongside columnar-projection-store-hardwood-"
                            + "processor on the compiler's annotation "
                            + "processor path, then clean and recompile.");
        }
        pending.clear();
    }

    private boolean hasAnnotation(Element element, String annotationName) {
        for (AnnotationMirror annotation : element.getAnnotationMirrors()) {
            Element annotationElement =
                    annotation.getAnnotationType().asElement();
            if (annotationElement instanceof TypeElement typeElement
                    && typeElement.getQualifiedName()
                            .contentEquals(annotationName)) {
                return true;
            }
        }
        return false;
    }

    private void error(Element element, String message) {
        messager.printMessage(Diagnostic.Kind.ERROR, message, element);
    }

    private static String qualify(String packageName, String simpleName) {
        return packageName.isEmpty()
                ? simpleName
                : packageName + "." + simpleName;
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static void line(StringBuilder source, String line) {
        source.append(line).append('\n');
    }

    private enum GenerationResult {
        WAITING,
        GENERATED,
        FAILED
    }

    private record PendingSchema(
            TypeElement schema,
            String binaryName,
            String packageName,
            String binarySimpleName,
            String loaderSimpleName,
            String loaderQualifiedName) {
    }

    private record Mapping(
            String readerAccessor,
            boolean primitive,
            String firstPhysicalType,
            String secondPhysicalType) {
    }

    private record ColumnBinding(
            String name,
            TypeMirror parameterType,
            Mapping mapping) {
    }
}
