// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hanchuanchuan/goInception/mysql"
)

var _ = yyLexer(&Scanner{})

// Pos represents the position of a token.
type Pos struct {
	Line   int
	Col    int
	Offset int
}

// Scanner implements the yyLexer interface.
type Scanner struct {
	r   reader
	buf bytes.Buffer

	errs         []error
	warns        []error
	stmtStartPos int

	// inBangComment is true if we are inside a `/*! ... */` block.
	// It is used to ignore a stray `*/` when scanning.
	inBangComment bool

	sqlMode mysql.SQLMode

	// If the lexer should recognize keywords for window function.
	// It may break the compatibility when support those keywords,
	// because some application may already use them as identifiers.
	supportWindowFunc bool

	// lastScanOffset indicates last offset returned by scan().
	// It's used to substring sql in syntax error message.
	lastScanOffset int

	// lastKeyword records the previous keyword returned by scan().
	// determine whether an optimizer hint should be parsed or ignored.
	lastKeyword int

	// hintPos records the start position of the previous optimizer hint.
	lastHintPos Pos
}

// Errors returns the errors and warns during a scan.
func (s *Scanner) Errors() (warns []error, errs []error) {
	return s.warns, s.errs
}

// reset resets the sql string to be scanned.
func (s *Scanner) reset(sql string) {
	s.r = reader{s: sql, p: Pos{Line: 1}}
	s.buf.Reset()
	s.errs = s.errs[:0]
	s.warns = s.warns[:0]
	s.stmtStartPos = 0
	s.inBangComment = false
	s.lastKeyword = 0
}

func (s *Scanner) stmtText() string {
	endPos := s.r.pos().Offset
	if s.r.s[endPos-1] == '\n' {
		endPos = endPos - 1 // trim new line
	}
	if s.r.s[s.stmtStartPos] == '\n' {
		s.stmtStartPos++
	}

	text := s.r.s[s.stmtStartPos:endPos]

	s.stmtStartPos = endPos
	return text
}

// Errorf tells scanner something is wrong.
// Scanner satisfies yyLexer interface which need this function.
func (s *Scanner) Errorf(format string, a ...interface{}) (err error) {
	str := fmt.Sprintf(format, a...)
	val := s.r.s[s.lastScanOffset:]
	var lenStr = ""
	if len(val) > 2048 {
		lenStr = "(total length " + strconv.Itoa(len(val)) + ")"
		val = val[:2048]
	}
	err = fmt.Errorf("line %d column %d near \"%s\"%s %s",
		s.r.p.Line, s.r.p.Col, val, str, lenStr)
	return
}

// AppendError sets error into scanner.
// Scanner satisfies yyLexer interface which need this function.
func (s *Scanner) AppendError(err error) {
	if err == nil {
		return
	}
	s.errs = append(s.errs, err)
}

func (s *Scanner) getNextToken() int {
	r := s.r
	tok, pos, lit := s.scan()
	if tok == identifier {
		tok = handleIdent(&yySymType{})
	}
	if tok == identifier {
		if tok1 := s.isTokenIdentifier(lit, pos.Offset); tok1 != 0 {
			tok = tok1
		}
	}
	s.r = r
	return tok
}

// Lex returns a token and store the token value in v.
// Scanner satisfies yyLexer interface.
// 0 and invalid are special token id this function would return:
// return 0 tells parser that scanner meets EOF,
// return invalid tells parser that scanner meets illegal character.
func (s *Scanner) Lex(v *yySymType) int {
	tok, pos, lit := s.scan()
	s.lastScanOffset = pos.Offset
	s.lastKeyword = 0
	v.offset = pos.Offset
	v.ident = lit
	if tok == identifier {
		tok = handleIdent(v)
	}
	if tok == identifier {
		if tok1 := s.isTokenIdentifier(lit, pos.Offset); tok1 != 0 {
			tok = tok1
			s.lastKeyword = tok1
		}
	}
	if s.sqlMode.HasANSIQuotesMode() &&
		tok == stringLit &&
		s.r.s[v.offset] == '"' {
		tok = identifier
	}

	if tok == pipes && !(s.sqlMode.HasPipesAsConcatMode()) {
		return pipesAsOr
	}

	if tok == not && s.sqlMode.HasHighNotPrecedenceMode() {
		return not2
	}

	if (tok == as || tok == member) && s.getNextToken() == of {
		_, pos, lit = s.scan()
		v.ident = fmt.Sprintf("%s %s", v.ident, lit)
		s.lastScanOffset = pos.Offset
		v.offset = pos.Offset
		if tok == as {
			s.lastKeyword = asof
			return asof
		}
		s.lastKeyword = memberof
		return memberof
	}

	switch tok {
	case intLit:
		return toInt(s, v, lit)
	case floatLit:
		return toFloat(s, v, lit)
	case decLit:
		return toDecimal(s, v, lit)
	case hexLit:
		return toHex(s, v, lit)
	case bitLit:
		return toBit(s, v, lit)
	case singleAtIdentifier, doubleAtIdentifier, cast, extract:
		v.item = lit
		return tok
	case null:
		v.item = nil
	case quotedIdentifier:
		tok = identifier
	}

	if tok == unicode.ReplacementChar {
		return invalid
	}

	return tok
}

// SetSQLMode sets the SQL mode for scanner.
func (s *Scanner) SetSQLMode(mode mysql.SQLMode) {
	s.sqlMode = mode
}

// GetSQLMode return the SQL mode of scanner.
func (s *Scanner) GetSQLMode() mysql.SQLMode {
	return s.sqlMode
}

// EnableWindowFunc controls whether the scanner recognize the keywords of window function.
func (s *Scanner) EnableWindowFunc(val bool) {
	s.supportWindowFunc = val
}

// InheritScanner returns a new scanner object which inherits configurations from the parent scanner.
func (s *Scanner) InheritScanner(sql string) *Scanner {
	return &Scanner{
		r:                 reader{s: sql},
		sqlMode:           s.sqlMode,
		supportWindowFunc: s.supportWindowFunc,
	}
}

// NewScanner returns a new scanner object.
func NewScanner(s string) *Scanner {
	return &Scanner{r: reader{s: s}}
}

func (s *Scanner) skipWhitespace() rune {
	return s.r.incAsLongAs(unicode.IsSpace)
}

func (s *Scanner) scan() (tok int, pos Pos, lit string) {
	ch0 := s.r.peek()
	if unicode.IsSpace(ch0) {
		ch0 = s.skipWhitespace()
	}
	pos = s.r.pos()
	if s.r.eof() {
		// when scanner meets EOF, the returned token should be 0,
		// because 0 is a special token id to remind the parser that stream is end.
		return 0, pos, ""
	}

	if !s.r.eof() && isIdentExtend(ch0) {
		return scanIdentifier(s)
	}

	// search a trie to get a token.
	node := &ruleTable
	for ch0 >= 0 && ch0 <= 255 {
		if node.childs[ch0] == nil || s.r.eof() {
			break
		}
		node = node.childs[ch0]
		if node.fn != nil {
			return node.fn(s)
		}
		s.r.inc()
		ch0 = s.r.peek()
	}

	tok, lit = node.token, s.r.data(&pos)
	return
}

func startWithXx(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	if s.r.peek() == '\'' {
		s.r.inc()
		s.scanHex()
		if s.r.peek() == '\'' {
			s.r.inc()
			tok, lit = hexLit, s.r.data(&pos)
		} else {
			tok = unicode.ReplacementChar
		}
		return
	}
	s.r.incAsLongAs(isIdentChar)
	tok, lit = identifier, s.r.data(&pos)
	return
}

func startWithNn(s *Scanner) (tok int, pos Pos, lit string) {
	tok, pos, lit = scanIdentifier(s)
	// The National Character Set, N'some text' or n'some test'.
	// See https://dev.mysql.com/doc/refman/5.7/en/string-literals.html
	// and https://dev.mysql.com/doc/refman/5.7/en/charset-national.html
	if lit == "N" || lit == "n" {
		if s.r.peek() == '\'' {
			tok = underscoreCS
			lit = "utf8"
		}
	}
	return
}

func startWithBb(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	if s.r.peek() == '\'' {
		s.r.inc()
		s.scanBit()
		if s.r.peek() == '\'' {
			s.r.inc()
			tok, lit = bitLit, s.r.data(&pos)
		} else {
			tok = unicode.ReplacementChar
		}
		return
	}
	s.r.incAsLongAs(isIdentChar)
	tok, lit = identifier, s.r.data(&pos)
	return
}

func startWithSharp(s *Scanner) (tok int, pos Pos, lit string) {
	s.r.incAsLongAs(func(ch rune) bool {
		return ch != '\n'
	})
	return s.scan()
}

func startWithDash(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	if strings.HasPrefix(s.r.s[pos.Offset:], "--") {
		remainLen := len(s.r.s[pos.Offset:])
		if remainLen == 2 || (remainLen > 2 && unicode.IsSpace(rune(s.r.s[pos.Offset+2]))) {
			s.r.incAsLongAs(func(ch rune) bool {
				return ch != '\n'
			})
			return s.scan()
		}
	}
	if strings.HasPrefix(s.r.s[pos.Offset:], "->>") {
		tok = juss
		s.r.incN(3)
		return
	}
	if strings.HasPrefix(s.r.s[pos.Offset:], "->") {
		tok = jss
		s.r.incN(2)
		return
	}
	tok = int('-')
	lit = "-"
	s.r.inc()
	return
}

func startWithSlash(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	if s.r.peek() != '*' {
		tok = int('/')
		lit = "/"
		return
	}

	isOptimizerHint := false
	currentCharIsStar := false

	s.r.inc() // we see '/*' so far.
	switch s.r.readByte() {
	case '!': // '/*!' MySQL-specific comments
		// See http://dev.mysql.com/doc/refman/5.7/en/comments.html
		// in '/*!', which we always recognize regardless of version.
		s.scanVersionDigits(5, 5)
		s.inBangComment = true
		return s.scan()

	case 'T': // '/*T' maybe TiDB-specific comments
		if s.r.peek() != '!' {
			// '/*TX' is just normal comment.
			break
		}
		s.r.inc()
		// in '/*T!', try to match the pattern '/*T![feature1,feature2,...]'.
		features := s.scanFeatureIDs()
		if SpecialCommentsController.ContainsAll(features) {
			s.inBangComment = true
			return s.scan()
		}

	case 'M': // '/*M' maybe MariaDB-specific comments
		// no special treatment for now.
		break

	case '+': // '/*+' optimizer hints
		// See https://dev.mysql.com/doc/refman/5.7/en/optimizer-hints.html
		if _, ok := hintedTokens[s.lastKeyword]; ok {
			// only recognize optimizers hints directly followed by certain
			// keywords like SELECT, INSERT, etc.
			isOptimizerHint = true
		}

	case '*': // '/**' if the next char is '/' it would close the comment.
		currentCharIsStar = true

	default:
		break
	}

	// standard C-like comment. read until we see '*/' then drop it.
	for {
		if currentCharIsStar || s.r.incAsLongAs(func(ch rune) bool { return ch != '*' }) == '*' {
			switch s.r.readByte() {
			case '/':
				// Meets */, means comment end.
				if isOptimizerHint {
					s.lastHintPos = pos
					return hintComment, pos, s.r.data(&pos)
				} else {
					return s.scan()
				}
			case 0:
				break
			case '*':
				currentCharIsStar = true
				continue
			default:
				currentCharIsStar = false
				continue
			}
		}
		// unclosed comment or other errors.
		s.errs = append(s.errs, ParseErrorWith(s.r.data(&pos), s.r.p.Line))
		return
	}
}

func startWithStar(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()

	// skip and exit '/*!' if we see '*/'
	if s.inBangComment && s.r.peek() == '/' {
		s.inBangComment = false
		s.r.inc()
		return s.scan()
	}
	// otherwise it is just a normal star.
	return '*', pos, "*"
}

func startWithAt(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()

	tok, lit = scanIdentifierOrString(s)
	switch tok {
	case '@':
		s.r.inc()
		stream := s.r.s[pos.Offset+2:]
		var prefix string
		for _, v := range []string{"global.", "session.", "local."} {
			if len(v) > len(stream) {
				continue
			}
			if strings.EqualFold(stream[:len(v)], v) {
				prefix = v
				s.r.incN(len(v))
				break
			}
		}
		tok, lit = scanIdentifierOrString(s)
		switch tok {
		case stringLit, quotedIdentifier:
			tok, lit = doubleAtIdentifier, "@@"+prefix+lit
		case identifier:
			tok, lit = doubleAtIdentifier, s.r.data(&pos)
		}
	case unicode.ReplacementChar:
		break
	default:
		tok = singleAtIdentifier
	}

	return
}

func scanIdentifier(s *Scanner) (int, Pos, string) {
	pos := s.r.pos()
	s.r.inc()
	s.r.incAsLongAs(isIdentChar)
	return identifier, pos, s.r.data(&pos)
}

func scanIdentifierOrString(s *Scanner) (tok int, lit string) {
	ch1 := s.r.peek()
	switch ch1 {
	case '\'', '"':
		tok, _, lit = startString(s)
	case '`':
		tok, _, lit = scanQuotedIdent(s)
	default:
		if isUserVarChar(ch1) {
			pos := s.r.pos()
			s.r.incAsLongAs(isUserVarChar)
			tok, lit = identifier, s.r.data(&pos)
		} else {
			tok = int(ch1)
		}
	}
	return
}

var (
	quotedIdentifier = -identifier
)

func scanQuotedIdent(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	s.buf.Reset()
	for {
		ch := s.r.readByte()
		if ch == unicode.ReplacementChar && s.r.eof() {
			tok = unicode.ReplacementChar
			return
		}
		if ch == '`' {
			if s.r.peek() != '`' {
				// don't return identifier in case that it's interpreted as keyword token later.
				tok, lit = quotedIdentifier, s.buf.String()
				return
			}
			s.r.inc()
		}
		s.buf.WriteRune(ch)
	}
}

func startString(s *Scanner) (tok int, pos Pos, lit string) {
	return s.scanString()
}

// lazyBuf is used to avoid allocation if possible.
// it has a useBuf field indicates whether bytes.Buffer is necessary. if
// useBuf is false, we can avoid calling bytes.Buffer.String(), which
// make a copy of data and cause allocation.
type lazyBuf struct {
	useBuf bool
	r      *reader
	b      *bytes.Buffer
	p      *Pos
}

func (mb *lazyBuf) setUseBuf(str string) {
	if !mb.useBuf {
		mb.useBuf = true
		mb.b.Reset()
		mb.b.WriteString(str)
	}
}

func (mb *lazyBuf) writeRune(r rune, w int) {
	if mb.useBuf {
		if w > 1 {
			mb.b.WriteRune(r)
		} else {
			mb.b.WriteByte(byte(r))
		}
	}
}

func (mb *lazyBuf) data() string {
	var lit string
	if mb.useBuf {
		lit = mb.b.String()
	} else {
		lit = mb.r.data(mb.p)
		lit = lit[1 : len(lit)-1]
	}
	return lit
}

func (s *Scanner) scanString() (tok int, pos Pos, lit string) {
	tok, pos = stringLit, s.r.pos()
	mb := lazyBuf{false, &s.r, &s.buf, &pos}
	ending := s.r.readByte()
	ch0 := s.r.peek()
	for !s.r.eof() {
		if ch0 == ending {
			s.r.inc()
			if s.r.peek() != ending {
				lit = mb.data()
				return
			}
			str := mb.r.data(&pos)
			mb.setUseBuf(str[1 : len(str)-1])
		} else if ch0 == '\\' && !s.sqlMode.HasNoBackslashEscapesMode() {
			mb.setUseBuf(mb.r.data(&pos)[1:])
			ch0 = handleEscape(s)
		}
		mb.writeRune(ch0, s.r.w)
		if !s.r.eof() {
			s.r.inc()
			ch0 = s.r.peek()
		}
	}

	tok = unicode.ReplacementChar
	return
}

// handleEscape handles the case in scanString when previous char is '\'.
func handleEscape(s *Scanner) rune {
	s.r.inc()
	ch0 := s.r.peek()
	/*
		\" \' \\ \n \0 \b \Z \r \t ==> escape to one char
		\% \_ ==> preserve both char
		other ==> remove \
	*/
	switch ch0 {
	case 'n':
		ch0 = '\n'
	case '0':
		ch0 = 0
	case 'b':
		ch0 = 8
	case 'Z':
		ch0 = 26
	case 'r':
		ch0 = '\r'
	case 't':
		ch0 = '\t'
	case '%', '_':
		s.buf.WriteByte('\\')
	}
	return ch0
}

func startWithNumber(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	tok = intLit
	ch0 := s.r.readByte()
	if ch0 == '0' {
		tok = intLit
		ch1 := s.r.peek()
		switch {
		case ch1 >= '0' && ch1 <= '7':
			s.r.inc()
			s.scanOct()
		case ch1 == 'x' || ch1 == 'X':
			s.r.inc()
			p1 := s.r.pos()
			s.scanHex()
			p2 := s.r.pos()
			// 0x, 0x7fz3 are identifier
			if p1 == p2 || isDigit(s.r.peek()) {
				s.r.incAsLongAs(isIdentChar)
				return identifier, pos, s.r.data(&pos)
			}
			tok = hexLit
		case ch1 == 'b':
			s.r.inc()
			p1 := s.r.pos()
			s.scanBit()
			p2 := s.r.pos()
			// 0b, 0b123, 0b1ab are identifier
			if p1 == p2 || isDigit(s.r.peek()) {
				s.r.incAsLongAs(isIdentChar)
				return identifier, pos, s.r.data(&pos)
			}
			tok = bitLit
		case ch1 == '.':
			return s.scanFloat(&pos)
		case ch1 == 'B':
			s.r.incAsLongAs(isIdentChar)
			return identifier, pos, s.r.data(&pos)
		}
	}

	s.scanDigits()
	ch0 = s.r.peek()
	if ch0 == '.' || ch0 == 'e' || ch0 == 'E' {
		return s.scanFloat(&pos)
	}

	// Identifiers may begin with a digit but unless quoted may not consist solely of digits.
	if !s.r.eof() && isIdentChar(ch0) {
		s.r.incAsLongAs(isIdentChar)
		return identifier, pos, s.r.data(&pos)
	}
	lit = s.r.data(&pos)
	return
}

func startWithDot(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	save := s.r.pos()
	if isDigit(s.r.peek()) {
		tok, _, lit = s.scanFloat(&pos)
		if s.r.eof() || !isIdentChar(s.r.peek()) {
			return
		}
		// Fail to parse a float, reset to dot.
		s.r.p = save
	}
	tok, lit = int('.'), "."
	return
}

func (s *Scanner) scanOct() {
	s.r.incAsLongAs(func(ch rune) bool {
		return ch >= '0' && ch <= '7'
	})
}

func (s *Scanner) scanHex() {
	s.r.incAsLongAs(func(ch rune) bool {
		return ch >= '0' && ch <= '9' ||
			ch >= 'a' && ch <= 'f' ||
			ch >= 'A' && ch <= 'F'
	})
}

func (s *Scanner) scanBit() {
	s.r.incAsLongAs(func(ch rune) bool {
		return ch == '0' || ch == '1'
	})
}

func (s *Scanner) scanFloat(beg *Pos) (tok int, pos Pos, lit string) {
	s.r.p = *beg
	// float = D1 . D2 e D3
	s.scanDigits()
	ch0 := s.r.peek()
	if ch0 == '.' {
		s.r.inc()
		s.scanDigits()
		ch0 = s.r.peek()
	}
	if ch0 == 'e' || ch0 == 'E' {
		s.r.inc()
		ch0 = s.r.peek()
		if ch0 == '-' || ch0 == '+' || isDigit(ch0) {
			s.r.inc()
			s.scanDigits()
			tok = floatLit
		} else {
			// D1 . D2 e XX when XX is not D3, parse the result to an identifier.
			// 9e9e = 9e9(float) + e(identifier)
			// 9est = 9est(identifier)
			s.r.incAsLongAs(isIdentChar)
			tok = identifier
		}
	} else {
		tok = decLit
	}
	pos, lit = *beg, s.r.data(beg)
	return
}

func (s *Scanner) scanDigits() string {
	pos := s.r.pos()
	s.r.incAsLongAs(isDigit)
	return s.r.data(&pos)
}

// scanVersionDigits scans for `min` to `max` digits (range inclusive) used in
// `/*!12345 ... */` comments.
func (s *Scanner) scanVersionDigits(min, max int) {
	pos := s.r.pos()
	for i := 0; i < max; i++ {
		ch := s.r.peek()
		if isDigit(ch) {
			s.r.inc()
		} else if i < min {
			s.r.p = pos
			return
		} else {
			break
		}
	}
}

func (s *Scanner) scanFeatureIDs() (featureIDs []string) {
	pos := s.r.pos()
	const init, expectChar, obtainChar = 0, 1, 2
	state := init
	var b strings.Builder
	for !s.r.eof() {
		ch := s.r.peek()
		s.r.inc()
		switch state {
		case init:
			if ch == '[' {
				state = expectChar
				break
			}
			s.r.p = pos
			return nil
		case expectChar:
			if isIdentChar(ch) {
				b.WriteRune(ch)
				state = obtainChar
				break
			}
			s.r.p = pos
			return nil
		case obtainChar:
			if isIdentChar(ch) {
				b.WriteRune(ch)
				state = obtainChar
				break
			} else if ch == ',' {
				featureIDs = append(featureIDs, b.String())
				b.Reset()
				state = expectChar
				break
			} else if ch == ']' {
				featureIDs = append(featureIDs, b.String())
				return featureIDs
			}
			s.r.p = pos
			return nil
		}
	}
	s.r.p = pos
	return nil
}

func (s *Scanner) lastErrorAsWarn() {
	if len(s.errs) == 0 {
		return
	}
	s.warns = append(s.warns, s.errs[len(s.errs)-1])
	s.errs = s.errs[:len(s.errs)-1]
}

type reader struct {
	s string
	p Pos
	w int
}

var eof = Pos{-1, -1, -1}

func (r *reader) eof() bool {
	return r.p.Offset >= len(r.s)
}

// peek() peeks a rune from underlying reader.
// if reader meets EOF, it will return unicode.ReplacementChar. to distinguish from
// the real unicode.ReplacementChar, the caller should call r.eof() again to check.
func (r *reader) peek() rune {
	if r.eof() {
		return unicode.ReplacementChar
	}
	v, w := rune(r.s[r.p.Offset]), 1
	switch {
	case v == 0:
		r.w = w
		return v // illegal UTF-8 encoding
	case v >= 0x80:
		v, w = utf8.DecodeRuneInString(r.s[r.p.Offset:])
		if v == utf8.RuneError && w == 1 {
			v = rune(r.s[r.p.Offset]) // illegal UTF-8 encoding
		}
	}
	r.w = w
	return v
}

// inc increase the position offset of the reader.
// peek must be called before calling inc!
func (r *reader) inc() {
	if r.s[r.p.Offset] == '\n' {
		r.p.Line++
		r.p.Col = 0
	}
	r.p.Offset += r.w
	r.p.Col++
}

func (r *reader) incN(n int) {
	for i := 0; i < n; i++ {
		r.inc()
	}
}

func (r *reader) readByte() (ch rune) {
	ch = r.peek()
	if ch == unicode.ReplacementChar && r.eof() {
		return
	}
	r.inc()
	return
}

func (r *reader) pos() Pos {
	return r.p
}

func (r *reader) data(from *Pos) string {
	return r.s[from.Offset:r.p.Offset]
}

func (r *reader) incAsLongAs(fn func(rune) bool) rune {
	for {
		ch := r.peek()
		if !fn(ch) {
			return ch
		}
		if ch == unicode.ReplacementChar && r.eof() {
			return 0
		}
		r.inc()
	}
}
