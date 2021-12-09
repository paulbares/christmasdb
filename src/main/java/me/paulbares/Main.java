package me.paulbares;

import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Main {
  public static void main(String[] args) {
    Map<String, List<String>> groups = new LinkedHashMap<>();
    groups.put("A", List.of("Base", "MDD Baisse"));
    groups.put("B", List.of("Base", "MDD Baisse Simu Sensi"));
    groups.put("C", List.of("Base", "MDD Baisse", "MDD Baisse Simu Sensi"));
//    Thread.currentThread().join();

    Map<String, Double> cells = new LinkedHashMap<>();
    cells.put("Base", 110.44985250737464d);
    cells.put("MDD Baisse", 107.1165191740413d);
    cells.put("MDD Baisse Simu Sensi", 102.94985250737463d);

    // See entry as row
//    cells.entrySet().stream().flatMap(row -> )

    //

    Map<String, Double> newCells = new LinkedHashMap<>();
    Map<String, Double> newCellsDelta = new LinkedHashMap<>();
    groups.forEach((group, subgroups) -> {
      boolean[] first = new boolean[] { true};
      subgroups.forEach(scenario -> {
        Double value = cells.get(scenario);
        newCells.put(group + "." + scenario, value);
        String previousScenario = findPreviousGroup(subgroups, scenario);
        newCellsDelta.put(group + "|" + scenario, first[0] ? 0 : value - newCells.get(group + "." + previousScenario));
        first[0] = false;
      });
    });

    System.out.println(cells);
    System.out.println(newCells);
    System.out.println(newCellsDelta);
  }

  @Nullable
  private static String findPreviousGroup(List<String> subgroups, String scenario) {
    String previousScenario = null;
    for (String subgroup : subgroups) {
      if(subgroup.equals(scenario)) {
        break;
      }
      previousScenario = subgroup;
    }
    return previousScenario;
  }
}
