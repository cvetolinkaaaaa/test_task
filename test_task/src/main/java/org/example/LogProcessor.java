package org.example;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.*;

public class LogProcessor {
    private static final Pattern LOG_PATTERN = Pattern.compile(
            "\\[(.*?)\\] (user\\d+) (balance inquiry|transferred|withdrew) (\\d+(?:\\.\\d+)?)(?: to (user\\d+))?"
    );
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String OUTPUT_DIR = "transactions_by_users";
    private static final String OP_BALANCE = "balance inquiry";
    private static final String OP_TRANSFER = "transferred";
    private static final String OP_WITHDRAW = "withdrew";

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Использование: java LogProcessor <путь_к_папке>");
            return;
        }
        String directoryPath = args[0];
        try {
            processLogFiles(directoryPath);
            System.out.println("Файлы сохранены в папке " + OUTPUT_DIR);
        } catch (IOException e) {
            System.err.println("Ошибка обработки логов: " + e.getMessage());
        }
    }

    private static void processLogFiles(String directoryPath) throws IOException {
        Path dirPath = Paths.get(directoryPath);
        if (!Files.isDirectory(dirPath)) {
            throw new IllegalArgumentException("Указанный путь не является папкой");
        }

        Path outputDir = dirPath.resolve(OUTPUT_DIR);
        Files.createDirectories(outputDir);

        Map<String, List<Transaction>> userTransactions = new HashMap<>();
        Map<String, BigDecimal> userBalances = new HashMap<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath, "*.log")) {
            for (Path filePath : stream) {
                processFile(filePath, userTransactions, userBalances);
            }
        }

        Date now = new Date();

        for (Map.Entry<String, List<Transaction>> entry : userTransactions.entrySet()) {
            String userId = entry.getKey();
            List<Transaction> transactions = entry.getValue();
            BigDecimal finalBalance = userBalances.getOrDefault(userId, BigDecimal.ZERO);

            transactions.sort(Comparator.naturalOrder());

            Path userLogPath = outputDir.resolve(userId + ".log");
            try (BufferedWriter writer = Files.newBufferedWriter(userLogPath)) {
                for (Transaction transaction : transactions) {
                    writer.write(transaction.rawLine);
                    writer.newLine();
                }
                String finalBalanceEntry = String.format("[%s] %s final balance %s",
                        DATE_FORMAT.format(now), userId, finalBalance.setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString());
                writer.write(finalBalanceEntry);
            }
        }
    }

    private static void processFile(Path filePath,
                                    Map<String, List<Transaction>> userTransactions,
                                    Map<String, BigDecimal> userBalances) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                processLine(line, userTransactions, userBalances);
            }
        }
    }

    private static void processLine(String line,
                                    Map<String, List<Transaction>> userTransactions,
                                    Map<String, BigDecimal> userBalances) {
        Matcher matcher = LOG_PATTERN.matcher(line);
        if (!matcher.find()) {
            System.err.println("Не удалось распарсить строку: " + line);
            return;
        }

        String dateTimeStr = matcher.group(1);
        String userId = matcher.group(2);
        String operation = matcher.group(3);
        BigDecimal amount = new BigDecimal(matcher.group(4));
        String targetUser = matcher.group(5);

        Date date;
        try {
            date = DATE_FORMAT.parse(dateTimeStr);
        } catch (ParseException e) {
            System.err.println("Ошибка разбора даты: " + dateTimeStr);
            return;
        }

        addTransaction(userTransactions, userId, new Transaction(date, line, userId));

        switch (operation) {
            case OP_BALANCE:
                break;
            case OP_TRANSFER:
                updateBalance(userBalances, userId, amount.negate());
                updateBalance(userBalances, targetUser, amount);
                String receivedEntry = String.format("[%s] %s received %s from %s",
                        dateTimeStr, targetUser,
                        amount.setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString(),
                        userId);
                addTransaction(userTransactions, targetUser,
                        new Transaction(date, receivedEntry, targetUser));
                break;
            case OP_WITHDRAW:
                updateBalance(userBalances, userId, amount.negate());
                break;
            default:
                System.err.println("Неизвестная операция: " + operation);
        }
    }

    private static void addTransaction(Map<String, List<Transaction>> userTransactions,
                                       String userId, Transaction transaction) {
        userTransactions.computeIfAbsent(userId, k -> new ArrayList<>()).add(transaction);
    }

    private static void updateBalance(Map<String, BigDecimal> userBalances,
                                      String userId, BigDecimal amount) {
        userBalances.put(userId, userBalances.getOrDefault(userId, BigDecimal.ZERO).add(amount));
    }

    private static class Transaction implements Comparable<Transaction> {
        Date date;
        String rawLine;
        String userId;

        Transaction(Date date, String rawLine, String userId) {
            this.date = date;
            this.rawLine = rawLine;
            this.userId = userId;
        }

        @Override
        public int compareTo(Transaction other) {
            return this.date.compareTo(other.date);
        }
    }
}
