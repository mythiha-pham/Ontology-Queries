import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.*;

public class QueryExecutor {
    public static void main(String[] args) {
        // Load RDF/XML file
        String inputFile = "TGAOntology.rdf";
        Model model = ModelFactory.createDefaultModel();
        model.read(inputFile);

        String prefixString = 
            "PREFIX : <http://www.semanticweb.org/lukas/ontologies/2025/4/TheGameAwards2020-2024#>\n" +
            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";
        String outputFile = "query_results.txt";

        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {
            // Show all award events with optional hosts and dates
            writer.println("=== All Award Events with Optional Hosts and Dates ===");
            String allEventsQuery =
                prefixString +
                "SELECT ?event ?host ?date\n" +
                "WHERE {\n" +
                "  ?event a :TGA .\n" +  
                "  OPTIONAL { ?event :Host ?host . }\n" +
                "  OPTIONAL { ?event :TGAEventDate ?date . }\n" +
                "}\n" +
                "ORDER BY ?event\n";                                                
            executeQuery(model, allEventsQuery, writer);

            // Show only award events that have known hosts and dates
            writer.println("=== Award Events with Known Hosts and Dates ===");
            String knownHostsQuery =
                prefixString +
                "SELECT ?event ?host ?date\n" +
                "WHERE {\n" +
                "  ?event a :TGA .\n" +
                "  ?event :Host ?host .\n" +
                "  ?event :TGAEventDate ?date .\n" +
                "  FILTER(BOUND(?host) && BOUND(?date))\n" +
                "}\n" +
                "ORDER BY ?event\n";
            executeQuery(model, knownHostsQuery, writer);

            // Which game has won the most awards in 2020?
            writer.println("=== Query for most awarded game in 2020 ===");
            String mostAwardedGameQuery =
                prefixString +
                "SELECT ?game (COUNT(?category) AS ?awardCount)\n" +
                "WHERE {\n" +
                "  ?game a :Game ;\n" +
                "        :won ?category .\n" +
                "  ?event a :TGA ;\n" +
                "       :TGAEventDate ?date ;\n" +
                "       :hasCategory ?category .\n" +
                "  FILTER(?date >= \"2020-01-01T00:00:00\"^^xsd:dateTime && ?date < \"2021-01-01T00:00:00\"^^xsd:dateTime)\n" +
                "}\n" +
                "GROUP BY ?game\n" +
                "ORDER BY DESC(?awardCount)\n" +
                "LIMIT 1\n";
            executeQuery(model, mostAwardedGameQuery, writer);

            // Which categories were not presented in 2020?
            writer.println("=== Categories Not Presented in 2020 ===");
            String missingCategoriesQuery =
                prefixString +
                "SELECT ?category\n" +
                "WHERE {\n" +
                "  ?category a :Category .\n" +
                "  FILTER NOT EXISTS {\n" +
                "    ?tga a :TGA ;\n" +
                "         :TGAEventDate ?date ;\n" +
                "         :hasCategory ?category .\n" +
                "    FILTER(?date >= \"2020-01-01T00:00:00\"^^xsd:dateTime && ?date < \"2021-01-01T00:00:00\"^^xsd:dateTime)\n" +
                "  }\n" +
                "}\n" +
                "ORDER BY ?category\n";
            executeQuery(model, missingCategoriesQuery, writer);

            // Which developer has won the most awards?
            writer.println("=== Developer with Most Awards ===");
            String developerAwardsQuery =
                prefixString +
                "SELECT ?developer (COUNT(DISTINCT ?game) AS ?awardCount)\n" +
                "WHERE {\n" +
                "  ?game a :Game ;\n" +
                "        :won ?category ;\n" +
                "        :Developer ?developerValue .\n" +
                "  BIND(str(?developerValue) AS ?developer)\n" +
                "}\n" +
                "GROUP BY ?developer\n" +
                "ORDER BY DESC(?awardCount)\n" +
                "LIMIT 1\n";
            executeQuery(model, developerAwardsQuery, writer); 

            // Which genre has the highest amount of award-winning titles?
            writer.println("=== Genre with Highest Number of Award-Winning Titles ===");
            String genreAwardsQuery =
                prefixString +
                "SELECT ?genre (COUNT(DISTINCT ?game) AS ?winCount)\n" +
                "WHERE {\n" +
                "  ?game a :Game ;\n" +
                "        :won ?category ;\n" +
                "        :Genre ?genreValue .\n" +
                "  BIND(str(?genreValue) AS ?genre)\n" +
                "}\n" +
                "GROUP BY ?genre\n" +
                "ORDER BY DESC(?winCount)\n" +
                "LIMIT 1\n";
            executeQuery(model, genreAwardsQuery, writer);

        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }

    // Execute SPARQL query and write results to file
    private static void executeQuery(Model model, String queryString, PrintWriter writer) {
        Query query = QueryFactory.create(queryString);
        try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            ResultSet results = qexec.execSelect();
            
            // Get column names
            List<String> varNames = results.getResultVars();
            
            // Store all rows for formatting
            List<List<String>> rows = new ArrayList<>();
            
            // Add header row
            List<String> headerRow = new ArrayList<>(varNames);
            rows.add(headerRow);
            
            // Process results
            while (results.hasNext()) {
                QuerySolution solution = results.next();
                List<String> row = new ArrayList<>();
                for (String varName : varNames) {
                    if (solution.get(varName) != null) {
                        String value = solution.get(varName).toString();
                        value = cleanValue(value);
                        row.add(value);
                    } else {
                        row.add("null");
                    }
                }
                rows.add(row);
            }
            
            // Calculate column widths
            int[] colWidths = new int[varNames.size()];
            for (int i = 0; i < varNames.size(); i++) {
                colWidths[i] = varNames.get(i).length();
                for (List<String> row : rows) {
                    colWidths[i] = Math.max(colWidths[i], row.get(i).length());
                }
            }
            
            // Print header
            printRow(headerRow, colWidths, writer);
            writer.println();
            
            // Print separator
            for (int width : colWidths) {
                writer.print("-".repeat(width + 2));
            }
            writer.println();
            
            // Print data rows
            for (int i = 1; i < rows.size(); i++) {
                printRow(rows.get(i), colWidths, writer);
            }
            
            writer.println(); // Add blank line between queries
        }
    }

    // Print a row with proper alignment
    private static void printRow(List<String> row, int[] colWidths, PrintWriter writer) {
        for (int i = 0; i < row.size(); i++) {
            String value = row.get(i);
            // Right-align numbers, left-align text
            if (value.matches("-?\\d+(\\.\\d+)?")) {
                writer.printf("%" + colWidths[i] + "s  ", value);
            } else {
                writer.printf("%-" + colWidths[i] + "s  ", value);
            }
        }
        writer.println();
    }

    // Clean up values
    private static String cleanValue(String value) {
        // Remove the full URL prefix
        if (value.contains("TheGameAwards2020-2024#")) {
            value = value.substring(value.lastIndexOf("#") + 1);
        }
        
        // Remove datatype information
        if (value.contains("^^")) {
            value = value.substring(0, value.indexOf("^^"));
        }
        
        // Remove quotes if present
        if (value.startsWith("\"") && value.endsWith("\"")) {
            value = value.substring(1, value.length() - 1);
        }
        return value;
    }
} 