package com.act.demo.service;

import com.act.demo.model.Employee;
import com.act.demo.model.Fraud;
import com.act.demo.model.IncentiveResult;
import com.act.demo.model.Transaction;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.annotation.PostConstruct;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class FileOperationService {

    @Value("${TRANSACTION_CSV_IMPORT_URL}")
    private String transactionCsvImportUrl;
    @Value("${FRAUD_CSV_IMPORT_URL}")
    private String fraudCsvImportUrl;
    @Value("${EMPLOYEE_CSV_IMPORT_URL}")
    private String employeeCsvImportUrl;
    @Value("${EXPORT_OUTPUT_CSV_URL}")
    private String exportOutputCsvUrl;

    @PostConstruct
    private void getFilteredEmployeeList() throws IOException {
        List<String> fraudEmployeeList = getAllFraud().stream().map(p -> p.getEmpId()).collect(Collectors.toList());
        List<Employee> filteredEmployeeList = getEmployeeFromCsv().stream()
                .filter(employee -> !fraudEmployeeList.contains(employee.getEmpId())).collect(Collectors.toList());
        List<IncentiveResult> output = getIncentiveResult(filteredEmployeeList, getAllTransaction());
        exportToCsv(output);
    }

    private List<Transaction> getAllTransaction() throws IOException {
        List<String[]> r = readFile(transactionCsvImportUrl);
        List<Transaction> fraudList = new ArrayList<>();
        for (int i = 1; i < r.size(); i++) {
            String[] arrays = r.get(i);
            Transaction transaction = new Transaction();
            transaction.setTransId(Long.valueOf(arrays[0]));
            transaction.setTransProductType(arrays[1]);
            transaction.setTransType(arrays[2]);
            transaction.setTransAmount(Long.valueOf(arrays[3]));
            if (!ObjectUtils.isEmpty(arrays[4])) {
                transaction.setTransParentId(Long.valueOf(arrays[4]));
            }
            transaction.setPriority(Boolean.valueOf(arrays[5]));
            transaction.setEmpId(arrays[6]);
            fraudList.add(transaction);
        }
        return fraudList;
    }

    private List<Fraud> getAllFraud() throws IOException {
        List<String[]> r = readFile(fraudCsvImportUrl);
        List<Fraud> fraudList = new ArrayList<>();
        for (int i = 1; i < r.size(); i++) {
            String[] arrays = r.get(i);
            Fraud fraud = new Fraud();
            fraud.setEmpId(arrays[0]);
            fraudList.add(fraud);
        }
        return fraudList;
    }

    private List<Employee> getEmployeeFromCsv() throws IOException {
        List<String[]> r = readFile(employeeCsvImportUrl);
        List<Employee> employeeList = new ArrayList<>();
        for (int i = 1; i < r.size(); i++) {
            String[] arrays = r.get(i);
            Employee employee = new Employee();
            employee.setEmpId(arrays[0]);
            employee.setName(arrays[1]);
            employee.setLocation(arrays[2]);
            employeeList.add(employee);
        }
        return employeeList;
    }

    private List<String[]> readFile(String fileName) throws IOException {
        List<String[]> r = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(fileName))) {
            r = reader.readAll();
        } catch (FileNotFoundException | CsvException e) {
            e.printStackTrace();
        }
        return r;
    }

    private void exportToCsv(List<IncentiveResult> output) {
        List<String[]> resultArray = new ArrayList<>();
        String[] header = {"EMPLOYEE ID", "EMPLOYEE NAME", "INCENTIVE AMOUNT"};
        resultArray.add(header);
        for (int i = 0; i < output.size(); i++) {
            IncentiveResult value = output.get(i);
            String[] arr = {value.getEmpId(), value.getEmployeeName(), value.getAmount().toString()};
            resultArray.add(arr);
        }
        String[] footer = {"", " LAST UPDATED", String.valueOf(LocalDateTime.now())};
        resultArray.add(footer);
        try (CSVWriter writer = new CSVWriter(new FileWriter(exportOutputCsvUrl))) {
            writer.writeAll(resultArray);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<IncentiveResult> getIncentiveResult(List<Employee> filteredEmployeeList, List<Transaction> allTransactionList) {
        List<IncentiveResult> output = new ArrayList<>();
        filteredEmployeeList.forEach(filteredEmployee -> {
            List<Transaction> transactionListForEmployee = allTransactionList.stream()
                    .filter(transaction -> transaction.getEmpId().equals(filteredEmployee.getEmpId())).collect(Collectors.toList());
            IncentiveResult insetiveOfEmployee = calculateIncentivesForEmployee(filteredEmployee, transactionListForEmployee);
            output.add(insetiveOfEmployee);
        });
        return output;
    }

    private IncentiveResult calculateIncentivesForEmployee(Employee filteredEmployee, List<Transaction> transactionListForEmployee) {
        IncentiveResult incentiveResult = new IncentiveResult();
        incentiveResult.setAmount(calculateAmountForTransactions(transactionListForEmployee, filteredEmployee.getLocation()));
        incentiveResult.setEmpId(filteredEmployee.getEmpId());
        incentiveResult.setEmployeeName(filteredEmployee.getName());
        return incentiveResult;
    }

    private Long calculateAmountForTransactions(List<Transaction> transactionListForEmployee, String location) {
        Map<Long, Long> transactionMap = new HashMap<>();
        List<Long> cancelledTransactionIds = new ArrayList<>();
        for (Transaction transaction : transactionListForEmployee) {
            if (transaction.getTransType().equals("CANCEL")) {
                cancelledTransactionIds.add(transaction.getTransId());
                if (transaction.getTransParentId() != null) {
                    cancelledTransactionIds.add(transaction.getTransParentId());
                }
            } else {
                transactionMap.put(transaction.getTransId(), giveProductTypeBasedAmount(transaction));
            }
        }
        Long totalAmountForTransactions = finalTransactionsAmount(transactionMap, cancelledTransactionIds);
        Long locationBasedIncentive = (totalAmountForTransactions == 0L) ? 0L : giveLocationWiseIncentive(location);
        return totalAmountForTransactions + locationBasedIncentive;
    }

    private Long finalTransactionsAmount(Map<Long, Long> transactionMap, List<Long> cancelledTransactionIds) {
        Long amount = 0L;
        for (Long cancelledTransactionId : cancelledTransactionIds) {
            transactionMap.put(cancelledTransactionId, 0L);
        }
        for (Long price : transactionMap.values()) {
            amount += price;
        }
        return amount;
    }

    private Long giveProductTypeBasedAmount(Transaction transaction) {
        Long amount;
        if (transaction.isPriority()) {
            return 200L;
        }
        switch (transaction.getTransProductType()) {
            case "LOAN":
                amount = 100L;
                break;
            case "FD":
                amount = 50L;
                break;
            case "RD":
                amount = 60L;
                break;
            case "SA":
                amount = 40L;
                break;
            default:
                amount = 10L;
        }
        return amount;
    }

    private Long giveLocationWiseIncentive(String location) {
        Long amount;
        switch (location) {
            case "Delhi":
                amount = 10L;
                break;
            case "Punjab":
                amount = 10L;
                break;
            case "Maharashtra":
                amount = 10L;
                break;
            case "Haryana":
                amount = 5L;
                break;
            case "Gujrat":
                amount = 5L;
                break;
            default:
                amount = 10L;
        }
        return amount;
    }

}
