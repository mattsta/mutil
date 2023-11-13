import pytest
from mutil.expand import expand_string_curly_braces as expand_string

# (thanks to gpt-4 for automated test case generation)


def test_basic_expansion():
    assert expand_string("Hello {Me,My} Name") == ["Hello Me Name", "Hello My Name"]


def test_empty_string():
    assert expand_string("") == [""]


def test_string_without_expansion():
    assert expand_string("Hello My Name") == ["Hello My Name"]


def test_multiple_expansions():
    assert expand_string("{Hi,Hello} {Me,My} Name") == [
        "Hi Me Name",
        "Hi My Name",
        "Hello Me Name",
        "Hello My Name",
    ]


def test_single_expansion_option():
    assert expand_string("Hello {My} Name") == ["Hello My Name"]


def test_multiple_same_expansions():
    assert expand_string("{Hello,Hi} {Hello,Hi}") == [
        "Hello Hello",
        "Hello Hi",
        "Hi Hello",
        "Hi Hi",
    ]


def test_expansion_with_space():
    assert expand_string("Hello {Me ,My} Name") == ["Hello Me  Name", "Hello My Name"]


def test_expansion_at_start():
    assert expand_string("{Hello,Hi} My Name") == ["Hello My Name", "Hi My Name"]


def test_expansion_at_end():
    assert expand_string("My Name is {John,Jane}") == [
        "My Name is John",
        "My Name is Jane",
    ]


def test_expansion_with_numbers():
    assert expand_string("My Number is {1,2,3}") == [
        "My Number is 1",
        "My Number is 2",
        "My Number is 3",
    ]


def test_expansion_with_symbols():
    assert expand_string("I {like,love} {#,!}") == [
        "I like #",
        "I like !",
        "I love #",
        "I love !",
    ]


def test_complex_expansions():
    assert expand_string("{A,B,C}{1,2,3}{!,@,#}") == [
        "A1!",
        "A1@",
        "A1#",
        "A2!",
        "A2@",
        "A2#",
        "A3!",
        "A3@",
        "A3#",
        "B1!",
        "B1@",
        "B1#",
        "B2!",
        "B2@",
        "B2#",
        "B3!",
        "B3@",
        "B3#",
        "C1!",
        "C1@",
        "C1#",
        "C2!",
        "C2@",
        "C2#",
        "C3!",
        "C3@",
        "C3#",
    ]


def test_number_expansion():
    assert expand_string("{1..3}") == ["1", "2", "3"]


def test_number_expansion_with_text():
    assert expand_string("Number{1..3}") == ["Number1", "Number2", "Number3"]


def test_multiple_number_expansions():
    assert expand_string("{1..2}{3..4}") == ["13", "14", "23", "24"]


def test_mixed_number_text_expansions():
    assert expand_string("{Hello,Hi}{1..2}") == ["Hello1", "Hello2", "Hi1", "Hi2"]


def test_backward_number_expansion():
    assert expand_string("{5..1}") == ["5", "4", "3", "2", "1"]


def test_backward_number_expansion_with_text():
    assert expand_string("Number{5..1}") == [
        "Number5",
        "Number4",
        "Number3",
        "Number2",
        "Number1",
    ]


def test_mixed_number_string_expansion():
    assert expand_string("{1..3,Hello,There}") == ["1", "2", "3", "Hello", "There"]


def test_mixed_number_string_expansion_with_text():
    assert expand_string("Say {1..3,Hello,There}") == [
        "Say 1",
        "Say 2",
        "Say 3",
        "Say Hello",
        "Say There",
    ]


def test_multi_mixed_number_string_expansion():
    assert expand_string("{1..2,Hello}{3..1,There}") == [
        "13",
        "12",
        "11",
        "1There",
        "23",
        "22",
        "21",
        "2There",
        "Hello3",
        "Hello2",
        "Hello1",
        "HelloThere",
    ]


def test_backward_mixed_number_string_expansion():
    assert expand_string("{5..1,Hello}") == ["5", "4", "3", "2", "1", "Hello"]


def test_backward_mixed_number_string_expansion_with_text():
    assert expand_string("Number{5..1,Hello}") == [
        "Number5",
        "Number4",
        "Number3",
        "Number2",
        "Number1",
        "NumberHello",
    ]


def test_normal_expansion():
    assert expand_string("Hello {World,Everyone}") == ["Hello World", "Hello Everyone"]


def test_increasing_range_expansion():
    assert expand_string("{1..3}") == ["1", "2", "3"]


def test_decreasing_range_expansion():
    assert expand_string("{3..1}") == ["3", "2", "1"]


def test_mixed_expansion():
    assert expand_string("{1..3,Hello,There}") == ["1", "2", "3", "Hello", "There"]


def test_mixed_expansion_decreasing():
    assert expand_string("{3..1,Hello,There}") == ["3", "2", "1", "Hello", "There"]


def test_nested_expansion():
    assert expand_string("{Hello,Hi} {1..3,Everyone}") == [
        "Hello 1",
        "Hello 2",
        "Hello 3",
        "Hello Everyone",
        "Hi 1",
        "Hi 2",
        "Hi 3",
        "Hi Everyone",
    ]


def test_no_expansion():
    assert expand_string("Hello World") == ["Hello World"]


def test_separate_items():
    assert expand_string("Hello {Me,My} Name") == ["Hello Me Name", "Hello My Name"]


def test_increasing_range():
    assert expand_string("{1..3}") == ["1", "2", "3"]


def test_decreasing_range():
    assert expand_string("{3..1}") == ["3", "2", "1"]


def test_multiple_groups():
    assert expand_string("{1..2} {A,B}") == ["1 A", "1 B", "2 A", "2 B"]


def test_mix_of_items_and_ranges():
    assert expand_string("{1..3,Hello,There}") == ["1", "2", "3", "Hello", "There"]
    assert expand_string("{5..1,Hello,There}") == [
        "5",
        "4",
        "3",
        "2",
        "1",
        "Hello",
        "There",
    ]


def test_complex_case():
    assert expand_string("ID{1..2}: {Passed,Failed}{1..2}") == [
        "ID1: Passed1",
        "ID1: Passed2",
        "ID1: Failed1",
        "ID1: Failed2",
        "ID2: Passed1",
        "ID2: Passed2",
        "ID2: Failed1",
        "ID2: Failed2",
    ]


def test_increasing_seq_with_strings():
    assert expand_string("{1..3,Hello,There}") == ["1", "2", "3", "Hello", "There"]


def test_decreasing_seq_with_strings():
    assert expand_string("{5..1,Hello,There}") == [
        "5",
        "4",
        "3",
        "2",
        "1",
        "Hello",
        "There",
    ]


def test_complex_seq_with_strings():
    assert expand_string("Say {1..3,Hello,There} {2..1,Goodbye}") == [
        "Say 1 2",
        "Say 1 1",
        "Say 1 Goodbye",
        "Say 2 2",
        "Say 2 1",
        "Say 2 Goodbye",
        "Say 3 2",
        "Say 3 1",
        "Say 3 Goodbye",
        "Say Hello 2",
        "Say Hello 1",
        "Say Hello Goodbye",
        "Say There 2",
        "Say There 1",
        "Say There Goodbye",
    ]


def test_increasing_seq_with_strings_and_trailing_values():
    assert expand_string("{1..3,Hello,There,21..16}") == [
        "1",
        "2",
        "3",
        "Hello",
        "There",
        "21",
        "20",
        "19",
        "18",
        "17",
        "16",
    ]
