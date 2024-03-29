package server

import (
	"testing"

	"github.com/IfanTsai/metis/config"
	"github.com/stretchr/testify/require"
)

func TestProcessInlineBuffer(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name:     "OK for simple string",
			query:    "+OK\r\n",
			expected: []string{"OK"},
		},
		{
			name:     "OK for error",
			query:    "-Error message\r\n",
			expected: []string{"Error", "message"},
		},
	}

	for index := range testCases {
		tc := testCases[index]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := NewClient(NewServer(&config.Config{}), 0)
			readQuery(client, tc.query)
			ok, err := processInlineBuffer(client)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.expected, client.args)
		})
	}
}

func TestProcessBulkBuffer(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name:     "Test for builk string",
			query:    "$5\r\nhello\r\n",
			expected: []string{"hello"},
		},
		{
			name:     "Test for builk string with CRLF",
			query:    "$7\r\nhello\r\n\r\n",
			expected: []string{"hello\r\n"},
		},
		{
			name:     "Test for empty bulk string",
			query:    "$0\r\n\r\n",
			expected: nil,
		},
	}

	for index := range testCases {
		tc := testCases[index]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := NewClient(NewServer(&config.Config{}), 0)
			readQuery(client, tc.query)
			ok, err := processBulkBuffer(client)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.expected, client.args)
		})
	}
}

func TestProcessMultiBulkBuffer(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name:     "Test for multi builk string",
			query:    "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
			expected: []string{"hello", "world"},
		},
		{
			name:     "Test for multi builk string with CRLF",
			query:    "*2\r\n$7\r\nhello\r\n\r\n$7\r\nworld\r\n\r\n",
			expected: []string{"hello\r\n", "world\r\n"},
		},
		{
			name:     "Test for empty multi bulk string",
			query:    "*0\r\n",
			expected: nil,
		},
	}

	for index := range testCases {
		tc := testCases[index]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := NewClient(NewServer(&config.Config{}), 0)
			readQuery(client, tc.query)
			ok, err := processMultiBulkBuffer(client)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.expected, client.args)
		})
	}
}

func readQuery(client *Client, query string) {
	for _, c := range query {
		client.queryBuf[client.queryLen] = byte(c)
		client.queryLen++
	}
}
