package server

import (
	"testing"

	"github.com/IfanTsai/metis/datastruct"
	"github.com/stretchr/testify/require"
)

func TestProcessInlineBuffer(t *testing.T) {
	testCases := []struct {
		name     string
		query    string
		expected []*datastruct.Object
	}{
		{
			name:     "OK for simple string",
			query:    "+OK\r\n",
			expected: []*datastruct.Object{datastruct.NewObject(datastruct.ObjectTypeString, "OK")},
		},
		{
			name:  "OK for error",
			query: "-Error message\r\n",
			expected: []*datastruct.Object{
				datastruct.NewObject(datastruct.ObjectTypeString, "Error"),
				datastruct.NewObject(datastruct.ObjectTypeString, "message"),
			},
		},
	}

	for index := range testCases {
		tc := testCases[index]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := NewClient(nil, 0)
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
		expected []*datastruct.Object
	}{
		{
			name:  "Test for builk string",
			query: "$5\r\nhello\r\n",
			expected: []*datastruct.Object{
				datastruct.NewObject(datastruct.ObjectTypeString, "hello"),
			},
		},
		{
			name:  "Test for builk string with CRLF",
			query: "$7\r\nhello\r\n\r\n",
			expected: []*datastruct.Object{
				datastruct.NewObject(datastruct.ObjectTypeString, "hello\r\n"),
			},
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

			client := NewClient(nil, 0)
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
		expected []*datastruct.Object
	}{
		{
			name:  "Test for multi builk string",
			query: "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
			expected: []*datastruct.Object{
				datastruct.NewObject(datastruct.ObjectTypeString, "hello"),
				datastruct.NewObject(datastruct.ObjectTypeString, "world"),
			},
		},
		{
			name:  "Test for multi builk string with CRLF",
			query: "*2\r\n$7\r\nhello\r\n\r\n$7\r\nworld\r\n\r\n",
			expected: []*datastruct.Object{
				datastruct.NewObject(datastruct.ObjectTypeString, "hello\r\n"),
				datastruct.NewObject(datastruct.ObjectTypeString, "world\r\n"),
			},
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

			client := NewClient(nil, 0)
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
