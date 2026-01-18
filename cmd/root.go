package cmd

import (
	"fmt"
	"os"

	"github.com/Jcho114/bitcask-go/store"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:           "bitcask-go",
	Short:         "A toy version of Bitcask in go",
	Long:          "A toy version of Bitcask in go",
	Args:          cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	RunE:          runRoot,
	SilenceErrors: true,
	SilenceUsage:  true,
}

func runRoot(cmd *cobra.Command, args []string) error {
	path := args[0]

	s, err := store.OpenStore(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err = s.RunReplit(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return nil
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
