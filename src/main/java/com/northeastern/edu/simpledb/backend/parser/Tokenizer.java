package com.northeastern.edu.simpledb.backend.parser;

import com.northeastern.edu.simpledb.common.Error;

//public class Tokenizer {
//    private enum State {
//        INIT, SYMBOL, QUOTE, TOKEN
//    }
//
//    private byte[] stat;
//    private int pos;
//    private String currentToken;
//    private boolean flushToken;
//    private Exception err;
//    private State currentState;
//
//    public Tokenizer(byte[] stat) {
//        this.stat = stat;
//        this.pos = 0;
//        this.currentToken = "";
//        this.flushToken = true;
//        this.currentState = State.INIT;
//    }
//
//    public String peek() throws Exception {
//        if (err != null) {
//            throw err;
//        }
//        if (flushToken) {
//            String token = null;
//            try {
//                token = next();
//            } catch (Exception e) {
//                err = e;
//                throw e;
//            }
//            currentToken = token;
//            flushToken = false;
//        }
//        return currentToken;
//    }
//
//    public void pop() {
//        flushToken = true;
//    }
//
//    public byte[] errStat() {
//        byte[] res = new byte[stat.length + 3];
//        System.arraycopy(stat, 0, res, 0, pos);
//        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
//        System.arraycopy(stat, pos, res, pos + 3, stat.length - pos);
//        return res;
//    }
//
//    private void popByte() {
//        pos++;
//        if (pos > stat.length) {
//            pos = stat.length;
//        }
//    }
//
//    private Byte peekByte() {
//        if (pos == stat.length) {
//            return null;
//        }
//        return stat[pos];
//    }
//
//    private String next() throws Exception {
//        if (err != null) {
//            throw err;
//        }
//
//        while (true) {
//            switch (currentState) {
//                case INIT:
//                    currentState = nextState();
//                    break;
//                case SYMBOL:
//                    return nextSymbolState();
//                case QUOTE:
//                    return nextQuoteState();
//                case TOKEN:
//                    return nextTokenState();
//            }
//        }
//    }
//
//    private State nextState() throws Exception {
//        Byte b = peekByte();
//        if (b == null) {
//            return State.INIT;
//        } else if (isSymbol(b)) {
//            return State.SYMBOL;
//        } else if (b == '"' || b == '\'') {
//            return State.QUOTE;
//        } else if (isAlphaBeta(b) || isDigit(b)) {
//            return State.TOKEN;
//        } else {
//            err = Error.InvalidCommandException;
//            throw err;
//        }
//    }
//
//    private String nextSymbolState() {
//        Byte b = peekByte();
//        popByte();
//        return new String(new byte[]{b});
//    }
//
//    private String nextTokenState() {
//        StringBuilder sb = new StringBuilder();
//        while (true) {
//            Byte b = peekByte();
//            if (b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
//                if (b != null && isBlank(b)) {
//                    popByte();
//                }
//                return sb.toString();
//            }
//            sb.append(new String(new byte[]{b}));
//            popByte();
//        }
//    }
//
//    private String nextQuoteState() throws Exception {
//        byte quote = peekByte();
//        popByte();
//        StringBuilder sb = new StringBuilder();
//        while (true) {
//            Byte b = peekByte();
//            if (b == null) {
//                err = Error.InvalidCommandException;
//                throw err;
//            }
//            if (b == quote) {
//                popByte();
//                break;
//            }
//            sb.append(new String(new byte[]{b}));
//            popByte();
//        }
//        return sb.toString();
//    }
//
//    private static boolean isDigit(byte b) {
//        return (b >= '0' && b <= '9');
//    }
//
//    private static boolean isAlphaBeta(byte b) {
//        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
//    }
//
//    private static boolean isSymbol(byte b) {
//        return (b == '>' || b == '<' || b == '=' || b == '*' ||
//                b == ',' || b == '(' || b == ')');
//    }
//
//    private static boolean isBlank(byte b) {
//        return (b == '\n' || b == ' ' || b == '\t');
//    }
//}

public class Tokenizer {
    private enum State {
        INIT, IN_SYMBOL, IN_QUOTE, IN_TOKEN, END
    }

    private enum TokenType {
        SYMBOL, QUOTE, TOKEN
    }

    private byte[] stat;
    private int pos;
    private String currentToken;
    private boolean flushToken;
    private Exception err;
    private State currentState;

    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
        this.currentState = State.INIT;
    }

    public String peek() throws Exception {
        if (err != null) {
            throw err;
        }
        if (flushToken) {
            String token = null;
            try {
                token = next();
            } catch (Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    public void pop() {
        flushToken = true;
    }

    public byte[] errStat() {
        byte[] res = new byte[stat.length + 3];
        System.arraycopy(stat, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(stat, pos, res, pos + 3, stat.length - pos);
        return res;
    }

    private void popByte() {
        pos++;
        if (pos > stat.length) {
            pos = stat.length;
        }
    }

    private Byte peekByte() {
        if (pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    private String next() throws Exception {
        if (err != null) {
            throw err;
        }

        while (true) {
            switch (currentState) {
                case INIT:
                    currentState = nextState();
                    break;
                case IN_SYMBOL:
                    return nextSymbolState();
                case IN_QUOTE:
                    return nextQuoteState();
                case IN_TOKEN:
                    return nextTokenState();
                case END:
                    return "";
            }
        }
    }

    private State nextState() throws Exception {
        while(true) {
            Byte b = peekByte();
            if (b == null) return State.END;
            if(!isBlank(b)) {
                break;
            }
            popByte();
        }

        Byte b = peekByte();
        if (b == null) {
            return State.INIT;
        } else if (isSymbol(b)) {
            return State.IN_SYMBOL;
        } else if (b == '"' || b == '\'') {
            return State.IN_QUOTE;
        } else if (isAlphaBeta(b) || isDigit(b)) {
            return State.IN_TOKEN;
        } else {
            err = Error.InvalidCommandException;
            throw err;
        }
    }

    private String nextSymbolState() {
        Byte b = peekByte();
        popByte();
        this.currentState = State.INIT;
        return new String(new byte[]{b});
    }

    private String nextTokenState() {
        StringBuilder sb = new StringBuilder();
        while (true) {
            Byte b = peekByte();
            if (b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                if (b != null && isBlank(b)) {
                    popByte();
                }
                this.currentState = State.INIT;
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
    }

    private String nextQuoteState() throws Exception {
        byte quote = peekByte();
        popByte();
        StringBuilder sb = new StringBuilder();
        while (true) {
            Byte b = peekByte();
            if (b == null) {
                err = Error.InvalidCommandException;
                throw err;
            }
            if (b == quote) {
                popByte();
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        this.currentState = State.INIT;
        return sb.toString();
    }

    private static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    private static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    private static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')');
    }

    private static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
