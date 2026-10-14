"""Tests for `archival_structures.parsers.ead_text.element_text` and its use for titles."""

from lxml import etree

from archival_structures.parsers.ead_text import element_text
from archival_structures.parsers.ead_tree import _text_of


def _el(xml):
    return etree.fromstring(xml)


def test_lb_inside_title_becomes_space():
    assert element_text(_el("<unittitle>eerste<lb/>deel</unittitle>")) == "eerste deel"


def test_lb_next_to_whitespace_gives_no_double_space():
    assert element_text(_el("<t>eerste <lb/>deel</t>")) == "eerste deel"
    assert element_text(_el("<t>eerste<lb/> deel</t>")) == "eerste deel"
    assert element_text(_el("<t>eerste \n<lb/>\n deel</t>")) == "eerste deel"


def test_lb_tail_and_nested_lb():
    assert element_text(_el("<t>a<unitdate>1<lb/>2</unitdate>b</t>")) == "a1 2b"


def test_unitdate_child_edge_whitespace_kept():
    assert element_text(_el("<t>derden<unitdate> September</unitdate></t>")) == "derden September"
    assert element_text(_el("<t>Brief <unitdate>1 maart </unitdate>van</t>")) == "Brief 1 maart van"


def test_nested_emphasis_and_edge_space():
    assert element_text(_el("<t>foo<emph>bar </emph>baz</t>")) == "foobar baz"
    assert element_text(_el("<t>a <emph>b <emph>c</emph> d</emph> e</t>")) == "a b c d e"


def test_word_split_by_markup_not_split():
    assert element_text(_el("<t>Ge<emph>heim</emph>e stukken</t>")) == "Geheime stukken"


def test_lb_before_punctuation_is_dropped():
    assert element_text(_el("<t>Eems<lb/>. Met</t>")) == "Eems. Met"
    assert element_text(_el("<t>Perzië]<lb/>).</t>")) == "Perzië])."
    assert element_text(_el("<t>a<lb/>(b)</t>")) == "a (b)"


def test_lb_before_quote_is_dropped():
    assert element_text(_el("<t>Dec 1674<lb/>'</t>")) == "Dec 1674'"
    assert element_text(_el("<t>[...]<lb/>'.</t>")) == "[...]'."
    assert element_text(_el('<t>liversatien"<lb/>\'</t>')) == 'liversatien"\''
    assert element_text(_el("<t>a<lb/>\u201d b</t>")) == "a\u201d b"
    assert element_text(_el("<t>a<lb/>\u2018b</t>")) == "a \u2018b"


def test_comment_and_pi_contribute_only_tail():
    assert element_text(_el("<t>a<!-- x -->b<?pi y?> c</t>")) == "ab c"


def test_empty_element():
    assert element_text(_el("<t/>")) == ""
    assert element_text(_el("<t>  <lb/> </t>")) == ""


def test_namespaced_lb():
    assert element_text(_el('<t xmlns="urn:isbn:1-931666-22-9">a<lb/>b</t>')) == "a b"


def test_text_of_wrapper():
    assert _text_of(None) is None
    assert _text_of(_el("<t> <lb/> </t>")) is None
    assert _text_of(_el("<t>eerste<lb/>deel</t>")) == "eerste deel"
